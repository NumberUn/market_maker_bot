import asyncio
import configparser
import json
import logging
import sys
import threading
import time
import uuid
import math
from datetime import datetime
from logging.config import dictConfig
from typing import List
import aiohttp

from arbitrage_finder import ArbitrageFinder, AP
from market_maker_counter import MarketFinder
from clients.core.all_clients import ALL_CLIENTS
from clients_markets_data import Clients_markets_data
from core.database import DB
from core.rabbit import Rabbit
from core.enums import AP_Status
from clients.core.enums import OrderStatus, ResponseStatus
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

leverage = float(config['SETTINGS']['LEVERAGE'])

init_time = time.time()


class MultiBot:
    __slots__ = ['deal_pause', 'cycle_parser_delay', 'max_order_size_usd', 'chosen_deal', 'profit_taker', 'shifts',
                 'rabbit', 'telegram', 'start_time', 'trade_exceptions', 'close_only_exchanges',
                 'available_balances', 'positions', 'clients', 'exchanges', 'env', 'db', 'tasks',
                 '_loop', 'loop_2', 'loop_3', 'last_orderbooks', 'time_start', 'time_parser', 'bot_launch_id',
                 'base_launch_config', 'instance_markets_amount', 'markets_data',
                 'launch_fields', 'setts', 'rates_file_name', 'markets', 'clients_markets_data', 'finder',
                 'clients_with_names', 'max_position_part', 'profit_close', 'potential_deals', 'limit_order_shift',
                 'deal_done_event', 'new_ap_event', 'new_db_record_event', 'ap_count_event', 'open_orders']

    def __init__(self):
        self.bot_launch_id = uuid.uuid4()
        self.db = None
        self.setts = config['SETTINGS']
        self.cycle_parser_delay = float(self.setts['CYCLE_PARSER_DELAY'])
        self.env = self.setts['ENV']
        self.trade_exceptions = {}
        self.close_only_exchanges = []
        self.instance_markets_amount = int(config['SETTINGS']['INSTANCE_MARKETS_AMOUNT'])
        self.launch_fields = ['env', 'target_profit', 'fee_exchange_1', 'fee_exchange_2', 'shift', 'orders_delay',
                              'max_order_usd', 'max_leverage', 'shift_use_flag']
        # ORDER CONFIGS
        self.deal_pause = float(self.setts['DEALS_PAUSE'])
        self.max_order_size_usd = int(self.setts['ORDER_SIZE'])
        self.profit_taker = float(self.setts['TARGET_PROFIT'])
        self.profit_close = float(self.setts['CLOSE_PROFIT'])
        self.max_position_part = float(self.setts['PERCENT_PER_MARKET'])
        self.limit_order_shift = int(self.setts['LIMIT_SHIFTS'])
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.clients = []
        self.telegram = Telegram()
        for exchange in self.exchanges:
            client = ALL_CLIENTS[exchange](self, keys=config[exchange], leverage=leverage,
                                           max_pos_part=self.max_position_part,
                                           ob_len=self.limit_order_shift + 1)
            self.clients.append(client)
        self.clients_with_names = {}
        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()
        self.available_balances = {}
        self.positions = {}
        self.clients_markets_data = Clients_markets_data(self.clients, self.setts['INSTANCE_NUM'],
                                                         self.instance_markets_amount)
        self.markets = self.clients_markets_data.get_instance_markets()
        self.markets_data = self.clients_markets_data.get_clients_data()

        self.base_launch_config = self.get_default_launch_config()
        self._loop = asyncio.new_event_loop()
        self.rabbit = Rabbit(self._loop)
        self.open_orders = {'EXCHANGE_NAME': {'COIN': ['id', "ORDER_DATA"],
                                              'OPEN_ORDERS': ['COIN_1', 'COIN_2']}}
        self.run_sub_processes()

    @try_exc_regular
    def get_default_launch_config(self):
        return {"env": self.setts['ENV'],
                "shift_use_flag": 0,
                "target_profit": 0.01,
                "orders_delay": 300,
                "max_order_usd": 50,
                "max_leverage": 2,
                'fee_exchange_1': self.clients[0].taker_fee,
                'fee_exchange_2': self.clients[1].taker_fee,
                'exchange_1': self.clients[0].EXCHANGE_NAME,
                'exchange_2': self.clients[1].EXCHANGE_NAME,
                'updated_flag': 1,
                'datetime_update': str(datetime.utcnow()),
                'ts_update': int(time.time() * 1000)}

    @try_exc_regular
    def run_main_process(self):
        while True:
            self._loop.run_until_complete(self.main_process())

    @try_exc_async
    async def main_process(self):
        await self.launch()
        while True:
            # print(f"CHECK ORDER STATUSES STARTED")
            await self.rabbit.setup_mq()
            tasks = [self._loop.create_task(self.__check_order_status()),
                     self._loop.create_task(self.rabbit.send_messages())]
            await asyncio.gather(*tasks, return_exceptions=True)
            # print(data)
            await asyncio.sleep(5)
            await self.rabbit.mq.close()

    @try_exc_regular
    def run_sub_processes(self):
        finder = MarketFinder(self.markets, self.clients_with_names, self)
        self.chosen_deal: AP
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            client.market_finder = finder
            client.run_updater()

    @staticmethod
    @try_exc_regular
    def run_await_in_thread(func, loop):
        loop.run_until_complete(func())

    @try_exc_async
    async def amend_maker_order(self, deal, exchange, order_id):
        pass

    @try_exc_async
    async def change_maker_order(self, deal, exchange, order_id):
        pass

    @try_exc_async
    async def delete_maker_order(self, exchange, order_id):
        pass

    @try_exc_async
    async def new_maker_order(self, exchange, deal):
        pass

    @try_exc_async
    async def launch(self):
        self.db = DB(self.rabbit)
        await self.db.setup_postgres()
        self.update_all_av_balances()
        self.update_all_positions_aggregates()
        print('CLIENTS MARKET DATA:')
        for exchange, exchange_data in self.markets_data.items():
            print(exchange, exchange_data['instance_markets_amt'])
        print('PARSER STARTED')
        self.telegram.send_bot_launch_message(self, TG_Groups.MainGroup)
        self.telegram.send_start_balance_message(self, TG_Groups.MainGroup)
        self.db.save_launch_balance(self)

    @try_exc_regular
    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(client.get_all_tops())
        return data

    @try_exc_regular
    def choose_deal(self, deal) -> AP:
        chosen_deal = None
        if deal_size_usd := self.if_tradable(deal.buy_exchange,
                                             deal.sell_exchange,
                                             deal.buy_market,
                                             deal.sell_market,
                                             deal.buy_price_parser,
                                             deal.sell_price_parser):
            deal.deal_size_usd_target = deal_size_usd
            chosen_deal = deal
        return chosen_deal

    @try_exc_regular
    def check_min_size(self, exchange, market, deal_avail_size_usd,
                       price, direction, context, send_flag: bool = True):
        min_size_amount = self.clients_with_names[exchange].instruments[market]['min_size']
        min_size_usd = min_size_amount * price
        if deal_avail_size_usd < min_size_usd:
            return False
        else:
            return True

    @try_exc_regular
    def get_close_only_exchanges(self):
        close_only_exchanges = []
        for exchange, available_balances in self.available_balances.items():
            if available_balances['buy'] == 0:
                close_only_exchanges.append(exchange)
        return close_only_exchanges

    @try_exc_regular
    def _get_available_balance(self, exchange, market, direction):
        if self.available_balances.get(exchange):
            if self.available_balances[exchange].get(market):
                avail_size = self.available_balances[exchange][market][direction]
            else:
                avail_size = self.available_balances[exchange][direction]
            return avail_size
        else:
            return 'updating'

    @try_exc_regular
    def if_tradable(self, buy_ex, sell_ex, buy_mrkt, sell_mrkt, buy_px, sell_px):
        avl_sz_buy_usd = self._get_available_balance(buy_ex, buy_mrkt, 'buy')
        avl_sz_sell_usd = self._get_available_balance(sell_ex, sell_mrkt, 'sell')
        if avl_sz_buy_usd == 'updating' or avl_sz_sell_usd == 'updating':
            return False
        max_deal_size_usd = min(avl_sz_buy_usd, avl_sz_sell_usd, self.max_order_size_usd)
        if not self.check_min_size(buy_ex, buy_mrkt, avl_sz_buy_usd, buy_px, 'buy', 'Bot work'):
            return False
        if not self.check_min_size(sell_ex, sell_mrkt, avl_sz_sell_usd, sell_px, 'sell', 'Bot work'):
            return False
        return max_deal_size_usd

    @try_exc_regular
    def fit_sizes_and_prices(self):
        deal_size_amount = self.chosen_deal.deal_size_usd_target / self.chosen_deal.ob_buy['asks'][0][0]
        step_size = max(self.chosen_deal.client_buy.instruments[self.chosen_deal.buy_market]['step_size'],
                        self.chosen_deal.client_sell.instruments[self.chosen_deal.sell_market]['step_size'])
        rounded_deal_size_amount = math.floor(deal_size_amount / step_size) * step_size
        # Округления до нуля произойти не может, потому, что deal_size_amount заведомо >= step_size
        self.chosen_deal.client_buy.amount = rounded_deal_size_amount
        self.chosen_deal.client_sell.amount = rounded_deal_size_amount
        buy_price_shifted = self.chosen_deal.ob_buy['asks'][self.limit_order_shift][0]
        sell_price_shifted = self.chosen_deal.ob_sell['bids'][self.limit_order_shift][0]
        self.chosen_deal.sell_price_shifted = sell_price_shifted
        self.chosen_deal.buy_price_shifted = buy_price_shifted
        # Здесь происходит уточнение и финализации размеров ордеров и их цен на клиентах
        self.chosen_deal.client_buy.fit_sizes(buy_price_shifted, self.chosen_deal.buy_market)
        self.chosen_deal.client_sell.fit_sizes(sell_price_shifted, self.chosen_deal.sell_market)
        # Сохраняем значения на объект AP. Именно по ним будет происходить попытка исполнения ордеров
        if not self.chosen_deal.client_buy.amount or not self.chosen_deal.client_sell.amount:
            self.telegram.send_message(f'STOP2.{rounded_deal_size_amount=}')
            return False
        #     return False
        return True

    @try_exc_async
    async def execute_deal(self):
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        self.chosen_deal.ts_orders_sent = time.time()
        self.chosen_deal.client_sell.symbol = self.chosen_deal.sell_market
        self.chosen_deal.client_sell.side = 'sell'
        self.chosen_deal.client_buy.symbol = self.chosen_deal.buy_market
        self.chosen_deal.client_buy.side = 'buy'
        self.chosen_deal.client_sell.deal = True
        self.chosen_deal.client_buy.deal = True
        while not self.chosen_deal.client_buy.response:
            await asyncio.sleep(0.01)
        while not self.chosen_deal.client_sell.response:
            await asyncio.sleep(0.01)
        responses = [self.chosen_deal.client_buy.response, self.chosen_deal.client_sell.response]
        self.chosen_deal.deal_size_amount_target = self.chosen_deal.client_buy.amount
        self.chosen_deal.profit_usd_target = self.chosen_deal.profit_rel_target * self.chosen_deal.deal_size_usd_target
        self.chosen_deal.buy_price_fitted = self.chosen_deal.client_buy.price
        self.chosen_deal.sell_price_fitted = self.chosen_deal.client_sell.price
        self.chosen_deal.buy_amount_target = self.chosen_deal.client_buy.amount
        self.chosen_deal.sell_amount_target = self.chosen_deal.client_sell.amount
        self.chosen_deal.buy_order_id, self.chosen_deal.sell_order_id = id1, id2
        self.chosen_deal.ts_orders_responses_received = time.time()
        self.chosen_deal.buy_order_place_time = (responses[0]['timestamp'] - self.chosen_deal.ts_orders_sent) / 1000
        self.chosen_deal.sell_order_place_time = (responses[1]['timestamp'] - self.chosen_deal.ts_orders_sent) / 1000
        self.chosen_deal.buy_order_id_exchange = responses[0]['exchange_order_id']
        self.chosen_deal.sell_order_id_exchange = responses[1]['exchange_order_id']
        self.chosen_deal.buy_order_place_status = responses[0]['status']
        self.chosen_deal.sell_order_place_status = responses[1]['status']
        message = f"Results of create_order requests:\n" \
                  f"{self.chosen_deal.buy_exchange=}\n" \
                  f"{self.chosen_deal.buy_market=}\n" \
                  f"{self.chosen_deal.sell_exchange=}\n" \
                  f"{self.chosen_deal.sell_market=}\n" \
                  f"Responses:\n{json.dumps(responses, indent=2)}"
        print(message)
        self.send_timings(responses)
        self.chosen_deal.client_buy.response = None
        self.chosen_deal.client_sell.response = None

    @staticmethod
    @try_exc_regular
    def count_pings(deal):
        if '.' in str(deal.ob_buy['timestamp']):
            ts_buy = deal.ts_orders_sent - deal.ob_buy['timestamp']
        else:
            ts_buy = deal.ts_orders_sent - deal.ob_buy['timestamp'] / 1000
        if '.' in str(deal.ob_sell['timestamp']):
            ts_sell = deal.ts_orders_sent - deal.ob_sell['timestamp']
        else:
            ts_sell = deal.ts_orders_sent - deal.ob_sell['timestamp'] / 1000
        buy_own_ts = deal.ts_orders_sent - deal.ob_buy['ts_ms']
        sell_own_ts = deal.ts_orders_sent - deal.ob_sell['ts_ms']
        return ts_buy, ts_sell, buy_own_ts, sell_own_ts

    @try_exc_regular
    def send_timings(self, responses):
        ts_buy, ts_sell, buy_own_ts, sell_own_ts = self.count_pings(self.chosen_deal)
        message = f'{self.chosen_deal.coin} DEAL TIMINGS:\n'
        message += f'{self.chosen_deal.sell_exchange} SELL OB:\n'
        message += f"OB AGE BY OB TS: {round(ts_sell, 5)} sec\n"
        message += f"OB AGE BY OWN TS: {round(sell_own_ts, 5)} sec\n"
        message += f'{self.chosen_deal.buy_exchange} BUY OB:\n'
        message += f"OB AGE BY OB TS: {round(ts_buy, 5)} sec\n"
        message += f"OB AGE BY OWN TS: {round(buy_own_ts, 5)} sec\n"
        countings = self.chosen_deal.ts_orders_sent - self.chosen_deal.start_processing
        message += f"TIME FROM START COUNTING: {round(countings, 5)} sec\n"
        orders_sendings = self.chosen_deal.ts_orders_responses_received - self.chosen_deal.ts_orders_sent
        message += f"ORDERS SENDING TIME: {round(orders_sendings, 5)} sec\n"
        ts_1 = responses[0]['timestamp'] - self.chosen_deal.ts_orders_sent
        ts_2 = responses[1]['timestamp'] - self.chosen_deal.ts_orders_sent
        message += f"{responses[0]['exchange_name']} ORDER TS: {ts_1} sec\n"
        message += f"{responses[1]['exchange_name']} ORDER TS: {ts_2} sec"
        self.telegram.send_message(message, TG_Groups.MainGroup)

    @try_exc_regular
    def get_ap_status(self):
        if self.chosen_deal.buy_order_execution_status in (OrderStatus.NEW, OrderStatus.PROCESSING):
            return AP_Status.UNDEFINED
        if self.chosen_deal.sell_order_execution_status in (OrderStatus.NEW, OrderStatus.PROCESSING):
            return AP_Status.UNDEFINED

        if self.chosen_deal.buy_order_execution_status == OrderStatus.FULLY_EXECUTED:
            if self.chosen_deal.sell_order_execution_status == OrderStatus.FULLY_EXECUTED:
                return AP_Status.SUCCESS

        if self.chosen_deal.buy_order_execution_status in (OrderStatus.NOT_PLACED, OrderStatus.NOT_EXECUTED):
            if self.chosen_deal.sell_order_execution_status in (OrderStatus.NOT_PLACED, OrderStatus.NOT_EXECUTED):
                return AP_Status.FULL_FAIL

        return AP_Status.DISBALANCE

    @try_exc_async
    async def notification_and_logging(self):

        ap_id = self.chosen_deal.ap_id
        client_buy, client_sell = self.chosen_deal.client_buy, self.chosen_deal.client_sell
        shifted_buy_px, shifted_sell_px = self.chosen_deal.buy_price_fitted, self.chosen_deal.sell_price_fitted
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        order_id_buy, order_id_sell = self.chosen_deal.buy_order_id, self.chosen_deal.sell_order_id
        buy_exchange_order_id, sell_exchange_order_id = self.chosen_deal.buy_order_id_exchange, self.chosen_deal.sell_order_id_exchange
        buy_order_place_time = self.chosen_deal.buy_order_place_time
        sell_order_place_time = self.chosen_deal.sell_order_place_time

        self.db.save_arbitrage_possibilities(self.chosen_deal)
        self.db.save_order(order_id_buy, buy_exchange_order_id, client_buy, 'buy', ap_id, buy_order_place_time,
                           shifted_buy_px, buy_market, self.env)
        self.db.save_order(order_id_sell, sell_exchange_order_id, client_sell, 'sell', ap_id, sell_order_place_time,
                           shifted_sell_px, sell_market, self.env)
        # self.new_db_record_event.set()

        if self.chosen_deal.buy_order_place_status == ResponseStatus.SUCCESS:
            # message = f'запрос инфы по ордеру, см. логи {self.chosen_deal.client_buy.EXCHANGE_NAME=}{buy_market=}{order_id_buy=}'
            # self.telegram.send_message(message)
            order_result = self.chosen_deal.client_buy.orders.get(buy_exchange_order_id, None)
            if not order_result:
                order_result = self.chosen_deal.client_buy.get_order_by_id(buy_market, buy_exchange_order_id)
            self.chosen_deal.buy_price_real = order_result['factual_price']
            self.chosen_deal.buy_amount_real = order_result['factual_amount_coin']
            self.chosen_deal.buy_order_execution_status = order_result['status']
            print(f'LABEL1 {order_result=}')
        else:
            self.chosen_deal.buy_order_execution_status = OrderStatus.NOT_PLACED
            self.telegram.send_order_error_message(self.env, buy_market, client_buy, order_id_buy, TG_Groups.Alerts)

        if self.chosen_deal.sell_order_place_status == ResponseStatus.SUCCESS:
            # message = f'запрос инфы по ордеру, см. логи{self.chosen_deal.client_sell.EXCHANGE_NAME=}{sell_market=}{order_id_buy=}'
            # self.telegram.send_message(message)
            order_result = self.chosen_deal.client_sell.orders.get(sell_exchange_order_id, None)
            if not order_result:
                order_result = self.chosen_deal.client_sell.get_order_by_id(sell_market, sell_exchange_order_id)
            self.chosen_deal.sell_price_real = order_result['factual_price']
            self.chosen_deal.sell_amount_real = order_result['factual_amount_coin']
            self.chosen_deal.sell_order_execution_status = order_result['status']
            print(f'LABEL2 {order_result=}')
        else:
            self.chosen_deal.sell_order_execution_status = OrderStatus.NOT_PLACED
            self.telegram.send_order_error_message(self.env, sell_market, client_sell, order_id_sell, TG_Groups.Alerts)
        if self.chosen_deal.sell_price_real > 0 and self.chosen_deal.buy_price_real > 0:
            self.chosen_deal.profit_rel_fact = (self.chosen_deal.sell_price_real - self.chosen_deal.buy_price_real) / \
                                               self.chosen_deal.buy_price_real
        self.chosen_deal.status = self.get_ap_status()
        self.db.update_balance_trigger('post-deal', ap_id, self.env)
        # self.new_db_record_event.set()
        self.telegram.send_ap_executed_message(self.chosen_deal, TG_Groups.MainGroup)

    @try_exc_regular
    def update_all_av_balances(self):
        for exchange, client in self.clients_with_names.items():
            self.available_balances.update({exchange: client.get_available_balance()})

    @try_exc_regular
    def update_all_positions_aggregates(self):
        for exchange, client in self.clients_with_names.items():
            markets = []
            total_pos = 0
            abs_pos = 0
            details = {}
            for market, position in client.get_positions().items():
                markets.append(market)
                total_pos += position['amount_usd']
                abs_pos += abs(position['amount_usd'])
                details.update({market: position})
            self.positions.update(
                {exchange: {'balance': int(round(client.get_balance())), 'total_pos': int(round(total_pos)),
                            'abs_pos': int(round(abs_pos)), 'markets': markets, 'position_details': details}})

    @try_exc_async
    async def __check_order_status(self):
        # Эта функция инициирует обновление данных по ордеру в базе,
        # когда от биржи в клиенте появляется обновление после создания ордера
        for client in self.clients:
            orders = client.orders.copy()

            for order_id, message in orders.items():
                self.rabbit.add_task_to_queue(message, "UPDATE_ORDERS")
                client.orders.pop(order_id)


if __name__ == '__main__':
    bot = MultiBot()
    bot.run_main_process()
    # asyncio.run(bot.main_process())

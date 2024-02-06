import asyncio
import configparser
import sys
import time
import uuid
from datetime import datetime
import math

from market_maker_counter import MarketFinder
from clients.core.all_clients import ALL_CLIENTS
from clients_markets_data import ClientsMarketData
from core.database import DB
from core.rabbit import Rabbit
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async
import random
import string

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

leverage = float(config['SETTINGS']['LEVERAGE'])

init_time = time.time()


class MultiBot:
    __slots__ = ['deal_pause', 'cycle_parser_delay', 'max_order_size_usd', 'chosen_deal', 'profit_open', 'shifts',
                 'rabbit', 'telegram', 'start_time', 'trade_exceptions', 'close_only_exchanges',
                 'available_balances', 'positions', 'clients', 'exchanges', 'env', 'db', 'tasks',
                 '_loop', 'loop_2', 'loop_3', 'last_orderbooks', 'time_start', 'time_parser', 'bot_launch_id',
                 'base_launch_config', 'instance_markets_amount', 'markets_data',
                 'launch_fields', 'setts', 'rates_file_name', 'markets', 'clients_markets_data', 'finder',
                 'clients_with_names', 'max_position_part', 'profit_close', 'potential_deals', 'limit_order_shift',
                 'deal_done_event', 'new_ap_event', 'new_db_record_event', 'ap_count_event', 'open_orders',
                 'mm_exchange', 'requests_in_progress', 'deleted_orders', 'count_ob_level', 'dump_orders', 'min_size',
                 'created_orders', 'deleted_orders']

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
        self.max_order_size_usd = int(self.setts['ORDER_SIZE'])
        self.min_size = int(self.setts['MIN_ORDER_SIZE'])
        self.max_position_part = float(self.setts['PERCENT_PER_MARKET'])
        self.limit_order_shift = int(self.setts['LIMIT_SHIFTS'])
        self.count_ob_level = int(self.setts['MAKER_SHIFTS'])
        self.profit_open = float(self.setts['PROFIT_OPEN'])
        self.profit_close = float(self.setts['PROFIT_CLOSE'])
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.mm_exchange = self.setts["MM_EXCHANGE"]
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
        self.clients_markets_data = ClientsMarketData(self.clients,
                                                      self.setts['INSTANCE_NUM'],
                                                      self.instance_markets_amount)
        self.markets = self.clients_markets_data.get_instance_markets()
        self.markets_data = self.clients_markets_data.get_clients_data()
        self.base_launch_config = self.get_default_launch_config()
        self._loop = asyncio.new_event_loop()
        self.rabbit = Rabbit(self._loop)
        self.open_orders = {'COIN-EXCHANGE': ['id', "ORDER_DATA"]}
        # self.dump_orders = {'COIN-EXCHANGE': ['id', "ORDER_DATA"]}
        self.run_sub_processes()
        self.requests_in_progress = []
        self.created_orders = set()
        self.deleted_orders = set()

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
            await self.rabbit.setup_mq()
            tasks = [self._loop.create_task(self.__check_order_status()),
                     self._loop.create_task(self.rabbit.send_messages())]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(5)
            await self.rabbit.mq.close()

    @try_exc_regular
    def run_sub_processes(self):
        finder = MarketFinder(self.markets, self.clients_with_names, self)
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            client.market_finder = finder
            client.run_updater()

    @try_exc_async
    async def check_for_non_legit_orders(self):
        all_canceled_orders = self.deleted_orders.copy()
        open_orders_set = {x[0] for x in self.open_orders.values()}
        all_canceled_orders.update(open_orders_set)
        if all_canceled_orders != self.created_orders:
            print(f"ALERT: NON LEGIT ORDERS: {self.created_orders - all_canceled_orders}")
            all_open_orders = self.clients_with_names[self.mm_exchange].get_all_orders()
            for order in all_open_orders:
                if order['orderID'] in self.created_orders - all_canceled_orders:
                    print(order)

    @staticmethod
    @try_exc_regular
    def run_await_in_thread(func, loop):
        loop.run_until_complete(func())

    @try_exc_async
    async def amend_maker_order(self, deal, coin, order_id):
        old_order = self.open_orders.get(coin + '-' + self.mm_exchange)
        if not old_order or old_order[0] != order_id:
            if old_order:
                print(f'AMEND MAKER ORDER WRONG ORDER_ID: {old_order}')
            await self.delete_maker_order(coin, order_id)
            self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
            return
        mm_client = self.clients_with_names[self.mm_exchange]
        market = mm_client.markets[coin]
        client_id = old_order[1]['client_id']
        price, size = mm_client.fit_sizes(deal['price'], deal['size'], market)
        # if price == old_order[1]['price']:
        #     self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
        #     return
        deal.update({'market': market,
                     'order_id': order_id,
                     'client_id': client_id,
                     'price': price,
                     'size': size,
                     'side': old_order[1]['side'],
                     'old_order_size': old_order[1]['size']})
        task = ['amend_order', deal]
        mm_client.async_tasks.append(task)
        for i in range(0, 1000):
            if resp := mm_client.responses.get(client_id):
                # print(f"AMEND: {old_order[0]} -> {resp['exchange_order_id']}")
                self.created_orders.update(resp['exchange_order_id'])
                self.open_orders.update({coin + '-' + self.mm_exchange: [resp['exchange_order_id'], deal]})
                mm_client.responses.pop(client_id)
                self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
                return
            await asyncio.sleep(0.001)
        await self.delete_maker_order(coin, order_id)
        self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
        self.open_orders.pop(coin + '-' + self.mm_exchange, None)
        # self.telegram.send_message(f"ALERT! MAKER ORDER WAS NOT AMENDED\n{deal}", TG_Groups.MainGroup)

    @try_exc_async
    async def delete_maker_order(self, coin, order_id):
        self.deleted_orders.update(order_id)
        mm_client = self.clients_with_names[self.mm_exchange]
        market = mm_client.markets[coin]
        task = ['cancel_order', {'market': market, 'order_id': order_id}]
        mm_client.async_tasks.append(task)
        # await asyncio.sleep(0.01)
        # self.requests_in_progress.remove(coin + '-' + self.mm_exchange)

    @try_exc_regular
    def precise_size(self, coin, size):
        step_size = max([x.instruments[x.markets[coin]]['step_size'] for x in self.clients if x.markets.get(coin)])
        perfect_size = math.floor(size / step_size) * step_size
        return perfect_size

    @staticmethod
    @try_exc_regular
    def id_generator(size=6, chars=string.ascii_letters):
        return ''.join(random.choice(chars) for _ in range(size))

    @try_exc_async
    async def new_maker_order(self, deal, coin):
        mm_client = self.clients_with_names[self.mm_exchange]
        market = mm_client.markets[coin]
        rand_id = self.id_generator()
        client_id = f'makerxxx{mm_client.EXCHANGE_NAME}xxx' + coin + 'xxx' + rand_id
        size = self.precise_size(coin, deal['size'])
        price, size = mm_client.fit_sizes(deal['price'], size, market)
        deal.update({'market': market,
                     'client_id': client_id,
                     'price': price,
                     'size': size})
        task = ['create_order', deal]
        mm_client.async_tasks.append(task)
        for i in range(0, 1000):
            if resp := mm_client.responses.get(client_id):
                self.created_orders.update(resp['exchange_order_id'])
                self.open_orders.update({coin + '-' + self.mm_exchange: [resp['exchange_order_id'], deal]})
                mm_client.responses.pop(client_id)
                self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
                await self.check_for_non_legit_orders()
                return
            await asyncio.sleep(0.001)
        self.requests_in_progress.remove(coin + '-' + self.mm_exchange)
        await self.check_for_non_legit_orders()
        # mm_client.cancel_all_orders(market)
        # print(f"NEW MAKER ORDER WAS NOT PLACED\n{deal=}")

    @try_exc_async
    async def hedge_maker_position(self, deal):
        deal_stored = self.open_orders.get(deal['coin'] + '-' + self.mm_exchange)
        side = 'buy' if deal['side'] == 'sell' else 'sell'
        best_market = None
        best_price = None
        best_client = None
        best_ob = None
        for client in self.clients:
            if self.mm_exchange == client.EXCHANGE_NAME:
                continue
            market = client.markets.get(deal['coin'])
            if market and client.instruments[market]['min_size'] <= deal['size']:
                ob = client.get_orderbook(market)
                price = ob['asks'][self.limit_order_shift][0] if side == 'buy' else ob['bids'][self.limit_order_shift][0]
                if best_price:
                    if side == 'buy':
                        if best_price > price:
                            best_price = price * 1.01
                            best_market = market
                            best_client = client
                            best_ob = ob
                    else:
                        if best_price < price:
                            best_price = price * 0.99
                            best_market = market
                            best_client = client
                            best_ob = ob
                else:
                    best_price = price * 1.01 if side == 'buy' else price * 0.99
                    best_market = market
                    best_client = client
                    best_ob = ob
        rand_id = self.id_generator()
        client_id = f'takerxxx{best_client.EXCHANGE_NAME}xxx' + deal['coin'] + 'xxx' + rand_id
        price, size = best_client.fit_sizes(best_price, deal['size'], best_market)
        best_client.async_tasks.append(['create_order', {'price': price,
                                                         'size': size,
                                                         'side': side,
                                                         'market': best_market,
                                                         'client_id': client_id}])
        await asyncio.sleep(0.1)
        if resp := best_client.responses.get(client_id):
            # if not deal_stored or deal_stored[0] != resp['exchange_order_id']:
            #     deal_stored = self.dump_orders.get(deal['coin'] + '-' + self.mm_exchange)
            print(f"STORED DEAL: {deal_stored}")
            best_client.responses.pop(client_id)
            results = self.sort_deal_response_data(deal, resp, best_ob, deal_stored)
            self.create_and_send_deal_report_message(results)
            return

        # best_client.cancel_all_orders(best_market)
        self.telegram.send_message(f"ALERT! TAKER DEAL WAS NOT PLACED\n{deal}", TG_Groups.MainGroup)

    @try_exc_regular
    def get_deal_direction_maker(self, positions, results):
        exchange_buy = results['maker exchange'] if results['maker side'] == 'buy' else results['taker exchange']
        exchange_sell = results['maker exchange'] if results['maker side'] == 'sell' else results['taker exchange']
        buy_market = self.clients_with_names[exchange_buy].markets[results['coin']]
        sell_market = self.clients_with_names[exchange_sell].markets[results['coin']]
        buy_close = False
        sell_close = False
        if pos_buy := positions[exchange_buy].get(buy_market):
            buy_close = True if pos_buy['amount_usd'] < 0 else False
        if pos_sell := positions[exchange_sell].get(sell_market):
            sell_close = True if pos_sell['amount_usd'] > 0 else False
        if buy_close and sell_close:
            return 'close'
        elif not buy_close and not sell_close:
            return 'open'
        else:
            return 'half_close'

    @try_exc_regular
    def create_and_send_deal_report_message(self, results: dict):
        # poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
        # direction = self.get_deal_direction_maker(poses, results)
        message = f'MAKER-TAKER DEAL EXECUTED\n{datetime.utcnow()}\n'
        for key, value in results.items():
            message += key.upper() + ': ' + str(value) + '\n'
        self.telegram.send_message(message, TG_Groups.MainGroup)

    @try_exc_regular
    def sort_deal_response_data(self, maker_deal: dict, taker_deal: dict, taker_ob: dict, deal_stored) -> dict:
        results = dict()
        last_upd = deal_stored[1]['last_update'] if deal_stored else 0
        target_price = deal_stored[1]['target'] if deal_stored else None
        direction = deal_stored[1]['direction'] if deal_stored else 'guess'
        results.update({'direction': direction,
                        'coin': maker_deal['coin'],
                        'order id stored': deal_stored[0] if deal_stored else deal_stored,
                        'order id filled': maker_deal['order_id'],
                        'last order update to fill': round(maker_deal['ts_ms'] - last_upd, 5),
                        'taker order ping': round(taker_deal['create_order_time'], 5),
                        'taker ws ob ping': round(taker_ob['ts_ms'] - taker_ob['timestamp'], 5),
                        'taker ob age': round(maker_deal['timestamp'] - taker_ob['timestamp'], 5),
                        'maker-taker ping': round(taker_deal['timestamp'] - maker_deal['timestamp'], 5),
                        'ping got fill -> send': round(taker_deal['time_order_sent'] - maker_deal['ts_ms'], 5),
                        'taker exchange': taker_deal['exchange_name'],
                        'maker exchange': self.mm_exchange,
                        'maker side': maker_deal['side'],
                        'maker price': maker_deal['price'],
                        'maker size': maker_deal['size'],
                        'taker price': taker_deal['price'],
                        'target taker price': target_price,
                        'taker size': taker_deal['size'],
                        'taker fee': round(self.clients_with_names[taker_deal['exchange_name']].taker_fee, 5),
                        'maker fee': round(self.clients_with_names[self.mm_exchange].maker_fee, 5)})
        fees = results['taker fee'] + results['maker fee']
        if results['taker price']:
            if maker_deal['side'] == 'buy':
                rel_profit = (results['taker price'] - results['maker price']) / results['maker price'] - fees
            else:
                rel_profit = (results['maker price'] - results['taker price']) / results['taker price'] - fees
        else:
            rel_profit = 0
        results.update({'relative profit': round(rel_profit, 6),
                        'absolute profit coin': round(rel_profit * results['taker size'], 8),
                        'absolute profit usd': round(rel_profit * results['taker size'] * results['taker price'], 6),
                        'disbalance coin': round(results['taker size'] - results['maker size'], 8),
                        'disbalance usd': round((results['taker size'] - results['maker size']) * results['taker price'], 6),
                        'total fee usd': round(fees * results['taker size'] * taker_deal['price'], 6)})
        return results

    @try_exc_async
    async def launch(self):
        self.db = DB(self.rabbit)
        await self.db.setup_postgres()
        await self.update_all_av_balances()
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
    def check_min_size(self, exchange, market, deal_avail_size_usd, price):
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
    def if_tradable(self, buy_ex, sell_ex, buy_mrkt, sell_mrkt, price):
        avl_sz_buy_usd = self._get_available_balance(buy_ex, buy_mrkt, 'buy')
        avl_sz_sell_usd = self._get_available_balance(sell_ex, sell_mrkt, 'sell')
        if avl_sz_buy_usd == 'updating' or avl_sz_sell_usd == 'updating':
            return False
        max_deal_size_usd = min(avl_sz_buy_usd, avl_sz_sell_usd, self.max_order_size_usd)
        if max_deal_size_usd < self.min_size:
            return False
        if not self.check_min_size(buy_ex, buy_mrkt, max_deal_size_usd, price):
            return False
        if not self.check_min_size(sell_ex, sell_mrkt, max_deal_size_usd, price):
            return False
        return max_deal_size_usd

    @try_exc_regular
    def fit_sz_and_px_maker(self, size, client, price, coin):
        market = client.markets[coin]
        price, amount = client.fit_sizes(price, size, market)
        return price, amount

    @try_exc_async
    async def update_all_av_balances(self):
        for exchange, client in self.clients_with_names.items():
            self.available_balances.update({exchange: client.get_available_balance()})
            print(f"UPDATED {exchange} avl balances: {client.get_available_balance()}")

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

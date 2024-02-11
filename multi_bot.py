import asyncio
import configparser
import sys
import time
import uuid
from datetime import datetime
import math

from market_maker_counter import MarketFinder
from arbitrage_finder import ArbitrageFinder
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
                 'created_orders', 'deleted_orders', 'market_maker', 'arbitrage', 'arbitrage_processing']

    def __init__(self):
        self.bot_launch_id = uuid.uuid4()
        self.db = None
        self.setts = config['SETTINGS']
        self.market_maker = True if self.setts['MARKET_MAKER'] == '1' else False
        self.arbitrage = True if self.setts['ARBITRAGE'] == '1' else False
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
        self.deal_pause = float(self.setts['DEALS_PAUSE'])
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
        self.dump_orders = {'COIN-EXCHANGE': ['id', "ORDER_DATA"]}
        self.arbitrage_processing = False
        self.run_sub_processes()
        self.requests_in_progress = dict()
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

    @try_exc_async
    async def run_arbitrage(self, deal):
        if self.arbitrage_processing:
            return
        size = self.if_tradable(deal['ex_buy'], deal['ex_sell'], deal['buy_mrkt'], deal['sell_mrkt'], deal['buy_px'])
        if not size:
            return
        unprecised_sz = min([size / deal['buy_px'], deal['buy_sz'], deal['sell_sz']])
        precised_sz = self.precise_size(deal['coin'], unprecised_sz)
        if precised_sz == 0:
            return
        self.arbitrage_processing = True
        rand_id = self.id_generator()
        client_id = f'takerxxx' + deal['coin'] + 'xxx' + rand_id
        buy_deal = {'price': deal['buy_px'], 'size': precised_sz, 'side': 'buy', 'market': deal['buy_mrkt'],
                    'client_id': client_id, 'hedge': True}
        sell_deal = {'price': deal['sell_px'], 'size': precised_sz, 'side': 'sell', 'market': deal['sell_mrkt'],
                     'client_id': client_id, 'hedge': True}
        deal['client_buy'].async_tasks.append(['create_order', buy_deal])
        deal['client_sell'].async_tasks.append(['create_order', sell_deal])
        ts_send = time.time()
        await asyncio.sleep(self.deal_pause)
        self.ap_deal_report(deal, client_id, precised_sz, ts_send)
        self.arbitrage_processing = False

    @try_exc_regular
    def ap_deal_report(self, deal, client_id, precised_sz, ts_send):
        resp_buy = None
        resp_sell = None
        real_profit = None
        ts_sent_buy_own = 0
        ts_sent_sell_own = 0
        ts_sent_buy_api = 0
        ts_sent_sell_api = 0
        trigger_side = 'sell' if deal['trigger_ex'] == deal['ex_sell'] else 'buy'
        count_to_send_ping = ts_send - deal['ts_start_counting']
        if trigger_side == 'sell':
            trigger_ping = deal['ob_sell_own_ts']
            inner_ping = ts_send - trigger_ping
            fetch_to_count_ping = deal['ts_start_counting'] - trigger_ping
        else:
            trigger_ping = deal['ob_buy_own_ts']
            inner_ping = ts_send - trigger_ping
            fetch_to_count_ping = deal['ts_start_counting'] - trigger_ping
        if deal['client_buy'].responses.get(client_id):
            resp_buy = deal['client_buy'].responses.get(client_id)
            deal['client_buy'].responses.pop(client_id)
            ts_sent_buy_own = resp_buy['time_order_sent']
            ts_sent_buy_api = resp_buy['timestamp']
        if deal['client_sell'].responses.get(client_id):
            resp_sell = deal['client_sell'].responses.get(client_id)
            deal['client_sell'].responses.pop(client_id)
            ts_sent_sell_own = resp_sell['time_order_sent']
            ts_sent_sell_api = resp_sell['timestamp']
        if resp_buy and resp_sell and resp_sell['price'] and resp_buy['price']:
            fees = deal['client_buy'].taker_fee + deal['client_sell'].taker_fee
            real_profit = round((resp_sell['price'] - resp_buy['price']) / resp_buy['price'] - fees, 5)
        message = f"TAKER DEAL EXECUTED | {deal['coin']}\n"
        message += f"DEAL DIRECTION: {deal['direction']}\n"
        message += f"BUY EXCHANGE: {deal['ex_buy']}\n"
        message += f"SELL EXCHANGE: {deal['ex_sell']}\n"
        message += f"TRIGGER EXCHANGE: {deal['trigger_ex']}\n"
        message += f"TRIGGER TYPE: {deal['trigger_type']}\n"
        message += f"TARGET BUY PRICE: {deal['buy_px']}\n"
        message += f"TARGET SELL PRICE: {deal['sell_px']}\n"
        message += f"TARGET SIZE: {precised_sz}\n"
        message += f"TARGET SIZE, USD: {round(precised_sz * deal['buy_px'], 2)}\n"
        message += f"TARGET PROFIT: {deal['profit']}\n"
        message += f"LIMIT PROFIT: {deal['target_profit']}\n"
        message += f"REAL BUY PRICE: {resp_buy['price'] if resp_buy else None}\n"
        message += f"REAL SELL PRICE: {resp_sell['price'] if resp_sell else None}\n"
        message += f"REAL BUY SIZE: {resp_buy['size'] if resp_buy else None}\n"
        message += f"REAL SELL SIZE: {resp_sell['size'] if resp_sell else None}\n"
        message += f"REAL PROFIT: {real_profit}\n"
        message += f"AGE BUY OB: {round(ts_send - deal['ob_buy_own_ts'], 5)}\n"
        message += f"AGE SELL OB: {round(ts_send - deal['ob_sell_own_ts'], 5)}\n"
        message += f"PING BUY ORDER: {round(resp_buy['create_order_time'], 5) if resp_buy else None}\n"
        message += f"PING SELL ORDER: {round(resp_sell['create_order_time'], 5) if resp_sell else None}\n"
        message += f"PING BUY OB API: {round(deal['ob_buy_api_ts'], 5)}\n"
        message += f"PING SELL OB API : {round(deal['ob_sell_api_ts'], 5)}\n"
        message += f"PING FETCH -> CREATED TASKS: {round(inner_ping, 5)}\n"
        message += f"PING FETCH -> COUNT: {round(fetch_to_count_ping, 5)}\n"
        message += f"PING START COUNTING -> SEND: {round(count_to_send_ping, 5)}\n"
        message += f"PING FETCH -> SENT BUY: {round(ts_sent_buy_own - trigger_ping, 5)}\n"
        message += f"PING FETCH -> SENT SELL: {round(ts_sent_sell_own - trigger_ping, 5)}\n"
        message += f"PING FETCH -> PLACED BUY: {round(ts_sent_buy_api - trigger_ping, 5)}\n"
        message += f"PING FETCH -> PLACED SELL: {round(ts_sent_sell_api - trigger_ping, 5)}\n"
        self.telegram.send_message(message, TG_Groups.MainGroup)

    @try_exc_regular
    def run_sub_processes(self):
        mm_finder = None
        ap_finder = None
        if self.market_maker:
            mm_finder = MarketFinder(self.markets, self.clients_with_names, self)
        if self.arbitrage:
            ap_finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_open, self.profit_close)
        for client in self.clients:
            client.markets_list = list([x for x in self.markets.keys() if client.markets.get(x)])
            client.market_finder = mm_finder
            client.finder = ap_finder
            client.run_updater()

    @try_exc_async
    async def check_for_non_legit_orders(self):
        time_start = time.time()
        all_canceled_orders = self.deleted_orders.copy()
        open_orders_set = {x[0] for x in self.open_orders.values()}
        all_canceled_orders.update(open_orders_set)
        if non_legit := all_canceled_orders - self.created_orders:
            print(f'CHECKING ORDERS TIME: {time.time() - time_start} sec')
            print(f"ALERT: NON LEGIT ORDERS: {non_legit}")
            all_open_orders = self.clients_with_names[self.mm_exchange].get_all_orders()
            for order in all_open_orders:
                if order['orderID'] in non_legit:
                    print(order)

    @staticmethod
    @try_exc_regular
    def run_await_in_thread(func, loop):
        loop.run_until_complete(func())

    @try_exc_async
    async def amend_maker_order(self, deal, coin, order_id):
        market_id = coin + '-' + self.mm_exchange
        old_order = self.open_orders.get(market_id)
        mm_client = self.clients_with_names[self.mm_exchange]
        market = mm_client.markets[coin]
        client_id = old_order[1]['client_id']
        price, size = mm_client.fit_sizes(deal['price'], deal['size'], market)
        deal.update({'market': market,
                     'order_id': order_id,
                     'client_id': client_id,
                     'price': price,
                     'size': size,
                     'side': old_order[1]['side'],
                     'old_order_size': old_order[1]['size']})
        task = ['amend_order', deal]
        mm_client.async_tasks.append(task)
        for i in range(0, 200):
            if resp := mm_client.responses.get(client_id):
                # print(f"AMEND: {old_order[0]} -> {resp['exchange_order_id']}")
                self.open_orders.update({market_id: [resp['exchange_order_id'], deal]})
                mm_client.responses.pop(client_id)
                self.requests_in_progress.update({market_id: False})
                return
            await asyncio.sleep(0.001)
        await self.delete_maker_order(coin, order_id)
        # self.telegram.send_message(f"ALERT! MAKER ORDER WAS NOT AMENDED\n{deal}", TG_Groups.MainGroup)

    @try_exc_async
    async def delete_maker_order(self, coin, order_id):
        # self.deleted_orders.update(order_id)
        mm_client = self.clients_with_names[self.mm_exchange]
        market = mm_client.markets[coin]
        task = ['cancel_order', {'market': market, 'order_id': order_id}]
        mm_client.async_tasks.append(task)
        market_id = coin + '-' + self.mm_exchange
        for i in range(0, 200):
            if mm_client.cancel_responses.get(order_id):
                self.requests_in_progress.update({market_id: False})
                # print(f"DELETE: {order_id}")
                # self.open_orders.pop(market_id, '')
                self.dump_orders.update({market_id: self.open_orders.pop(market_id, '')})
                mm_client.cancel_responses.pop(order_id, '')
                return
            await asyncio.sleep(0.001)
        self.requests_in_progress.update({market_id: False})
        # print(f"ALERT! MAKER ORDER WASN'T DELETED: {coin + '-' + self.mm_exchange} {order_id}")

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
        market_id = coin + '-' + self.mm_exchange
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
        for i in range(0, 200):
            if resp := mm_client.responses.get(client_id):
                # print(f"CREATE: {self.open_orders.get(market_id, [''])[0]} -> {resp['exchange_order_id']}")
                self.open_orders.update({market_id: [resp['exchange_order_id'], deal]})
                mm_client.responses.pop(client_id)
                self.requests_in_progress.update({market_id: False})
                return
            await asyncio.sleep(0.001)
        self.requests_in_progress.update({market_id: False})
        # print(f"NEW MAKER ORDER WAS NOT PLACED\n{deal=}")

    @try_exc_async
    async def hedge_maker_position(self, deal):
        mrkt_id = deal['coin'] + '-' + self.mm_exchange
        self.requests_in_progress.update({mrkt_id: True})
        deal_mem = self.open_orders.get(mrkt_id)
        dump_deal_mem = self.dump_orders.get(mrkt_id)
        side = 'buy' if deal['side'] == 'sell' else 'sell'
        best_market = None
        best_price = None
        top_clnt = None
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
                            top_clnt = client
                            best_ob = ob
                    else:
                        if best_price < price:
                            best_price = price * 0.99
                            best_market = market
                            top_clnt = client
                            best_ob = ob
                else:
                    best_price = price * 1.01 if side == 'buy' else price * 0.99
                    best_market = market
                    top_clnt = client
                    best_ob = ob
        rand_id = self.id_generator()
        client_id = f'mtakerxxx{top_clnt.EXCHANGE_NAME}xxx' + deal['coin'] + 'xxx' + rand_id
        price, size = top_clnt.fit_sizes(best_price, deal['size'], best_market)
        top_clnt.async_tasks.append(['create_order', {'price': price,
                                                      'size': size,
                                                      'side': side,
                                                      'market': best_market,
                                                      'client_id': client_id,
                                                      'hedge': True}])
        loop = asyncio.get_event_loop()
        loop.create_task(self.get_resp_report_deal(top_clnt, client_id, deal_mem, dump_deal_mem, deal, best_ob, mrkt_id))

    @try_exc_async
    async def get_resp_report_deal(self, top_clnt, client_id, deal_mem, dump_deal_mem, deal, best_ob, mrkt_id):
        await asyncio.sleep(0.02)
        self.requests_in_progress.update({mrkt_id: False})
        await asyncio.sleep(0.2)
        if resp := top_clnt.responses.get(client_id):
            if not deal_mem:
                deal_mem = dump_deal_mem
            print(f"STORED DEAL: {deal_mem}")
            print(f"DUMP DEAL: {dump_deal_mem}")
            top_clnt.responses.pop(client_id)
            results = self.sort_deal_response_data(deal, resp, best_ob, deal_mem)
            self.create_and_send_deal_report_message(results)
            return
        self.telegram.send_message(f"ALERT! TAKER DEAL WAS NOT PLACED\n{deal}", TG_Groups.MainGroup)

    # @try_exc_regular
    # def get_deal_direction_maker(self, positions, results):
    #     exchange_buy = results['maker exchange'] if results['maker side'] == 'buy' else results['taker exchange']
    #     exchange_sell = results['maker exchange'] if results['maker side'] == 'sell' else results['taker exchange']
    #     buy_market = self.clients_with_names[exchange_buy].markets[results['coin']]
    #     sell_market = self.clients_with_names[exchange_sell].markets[results['coin']]
    #     buy_close = False
    #     sell_close = False
    #     if pos_buy := positions[exchange_buy].get(buy_market):
    #         buy_close = True if pos_buy['amount_usd'] < 0 else False
    #     if pos_sell := positions[exchange_sell].get(sell_market):
    #         sell_close = True if pos_sell['amount_usd'] > 0 else False
    #     if buy_close and sell_close:
    #         return 'close'
    #     elif not buy_close and not sell_close:
    #         return 'open'
    #     else:
    #         return 'half_close'

    @try_exc_regular
    def create_and_send_deal_report_message(self, results: dict):
        message = f'MAKER-TAKER DEAL EXECUTED\n{datetime.utcnow()}\n'
        for key, value in results.items():
            message += key.upper() + ': ' + str(value) + '\n'
        self.telegram.send_message(message, TG_Groups.MainGroup)

    @try_exc_regular
    def sort_deal_response_data(self, maker_deal: dict, taker_deal: dict, taker_ob: dict, deal_mem) -> dict:
        results = dict()
        last_upd = deal_mem[1]['last_update'] if deal_mem else 0
        target_price = deal_mem[1]['target'] if deal_mem else None
        direction = deal_mem[1]['direction'] if deal_mem else 'guess'
        results.update({'direction': direction,
                        'coin': maker_deal['coin'],
                        'maker fill type': maker_deal['type'],
                        'order id stored': deal_mem[0] if deal_mem else deal_mem,
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
        return int(max_deal_size_usd)

    @try_exc_regular
    def fit_sz_and_px_maker(self, size, client, price, coin):
        market = client.markets[coin]
        price, amount = client.fit_sizes(price, size, market)
        return price, amount

    @try_exc_async
    async def update_all_av_balances(self):
        for exchange, client in self.clients_with_names.items():
            self.available_balances.update({exchange: client.get_available_balance()})
            # print(f"UPDATED {exchange} avl balances: {client.get_available_balance()}")

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

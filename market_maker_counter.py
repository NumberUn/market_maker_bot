import asyncio
import uuid
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async
import time
import json


class MarketFinder:

    def __init__(self, markets, clients_with_names, multibot):
        self.multibot = multibot
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.taker_fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.maker_fees = {x: y.maker_fee for x, y in self.clients_with_names.items()}

    def get_active_deal(self, coin):
        for exchange in self.multibot.open_orders:
            if order := self.multibot.open_orders[exchange].get(coin):
                return order
            else:
                return False

    @try_exc_regular
    def get_pings(self, ob_buy, ob_sell):
        now_ts = time.time()
        if not ob_buy or not ob_sell:
            return None
        if not ob_buy.get('bids') or not ob_buy.get('asks'):
            return None
        if not ob_sell.get('bids') or not ob_sell.get('asks'):
            return None
        if isinstance(ob_buy['timestamp'], float):
            ts_buy = now_ts - ob_buy['timestamp']
        else:
            ts_buy = now_ts - ob_buy['timestamp'] / 1000
        if isinstance(ob_sell['timestamp'], float):
            ts_sell = now_ts - ob_sell['timestamp']
        else:
            ts_sell = now_ts - ob_sell['timestamp'] / 1000
        return [ts_buy, ts_sell]

    @try_exc_regular
    def change_order(self, deal, exchange, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.change_maker_order(deal, exchange, order_id))

    @try_exc_regular
    def amend_order(self, deal, exchange, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.amend_maker_order(deal, exchange, order_id))

    @try_exc_regular
    def delete_order(self, exchange, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.delete_maker_order(exchange, order_id))

    @try_exc_regular
    def new_order(self, deal, exchange):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.new_maker_order(deal, exchange))

    @try_exc_async
    async def count_one_coin(self, coin, exchange):
        buy_deals = []
        sell_deals = []
        for exchange_buy, client_buy in self.clients_with_names.items():
            for exchange_sell, client_sell in self.clients_with_names.items():
                if exchange_buy == exchange_sell:
                    continue
                if exchange not in [exchange_buy, exchange_sell]:
                    continue
                if buy_mrkt := client_buy.markets.get(coin):
                    if sell_mrkt := client_sell.markets.get(coin):
                        ob_buy = client_buy.get_orderbook(buy_mrkt)
                        ob_sell = client_sell.get_orderbook(sell_mrkt)
                        pings = self.get_pings()
                        if not pings:
                            continue
                        ts_buy = pings[0]
                        ts_sell = pings[1]
                        # BUY SIDE COUNTINGS
                        if ts_buy < ts_sell and ts_buy < 0.050:
                            tick = client_buy.instruments[buy_mrkt]['tick_size']
                            buy_range = [ob_buy['bids'][0][0] + tick, ob_buy['asks'][0][0] - tick]
                            sell_px = ob_sell['bids'][2][0]
                            fees = self.maker_fees[exchange_buy] + self.taker_fees[exchange_sell]
                            zero_profit_buy_px = sell_px * (1 - fees)
                            if zero_profit_buy_px > buy_range[1]:
                                buy_deals.append({'mrkt': buy_mrkt, 'target': ob_sell['bids'][2],
                                                  'range': buy_range, 'ex': exchange_buy,
                                                  'op_ex': exchange_sell})
                            elif buy_range[0] < zero_profit_buy_px < buy_range[1]:
                                buy_range = [buy_range[0], zero_profit_buy_px]
                                buy_deals.append({'mrkt': buy_mrkt, 'target': ob_sell['bids'][2],
                                                  'range': buy_range, 'ex': exchange_buy,
                                                  'op_ex': exchange_sell})
                            # else:
                            #     if active_deal and active_deal['side'] == 'buy':
                            #         self.delete_order(exchange_buy, active_deal['order_id'])
                            #     continue
                            # deals.append({'side': 'buy', 'mrkt': buy_mrkt, 'price': buy_px,
                            #               'sz': ob_sell['bids'][2][1], 'range': buy_range})
                            # if active_deal:
                            #     if active_deal['side'] == 'buy':
                            #         if buy_range[0] < active_deal['price'] < buy_range[1]:
                            #             continue
                            #         else:
                            #             self.amend_order(deal, exchange_buy, active_deal['order_id'])
                            #     else:
                            #         self.change_order(deal, exchange_buy, active_deal['order_id'])
                            # else:
                            #     self.new_order(deal, exchange_buy)
                        # SELL SIDE COUNTINGS
                        if ts_buy > ts_sell and ts_sell < 0.050:
                            tick = client_sell.instruments[sell_mrkt]['tick_size']
                            sell_range = [ob_sell['asks'][0][0] - tick, ob_sell['bids'][0][0] + tick]
                            buy_px = ob_buy['asks'][2][0]
                            fees = self.maker_fees[exchange_sell] + self.taker_fees[exchange_buy]
                            zero_profit_sell_px = buy_px * (1 + fees)
                            if zero_profit_sell_px < sell_range[1]:
                                sell_deals.append({'mrkt': sell_mrkt,'target': ob_buy['asks'][2],
                                                   'range': sell_range, 'ex': exchange_sell,
                                                   'op_ex': exchange_buy})
                            elif sell_range[0] > zero_profit_sell_px > sell_range[1]:
                                sell_range = [sell_range[0], zero_profit_sell_px]
                                sell_deals.append({'mrkt': sell_mrkt, 'target': ob_buy['asks'][2],
                                                   'range': sell_range, 'ex': exchange_sell,
                                                   'op_ex': exchange_buy})
                            # else:
                            #     if active_deal and active_deal['side'] == 'sell':
                            #         self.delete_order(exchange_sell, active_deal['order_id'])
                            #     continue
                            # if sell_px:
                            #     deals.append({'side': 'sell', 'mrkt': sell_mrkt, 'price': sell_px,
                            #                   'sz': ob_buy['asks'][2][1], 'range': sell_range})
    def choose_maker_order(self, sell_deals, buy_deals, coin):
        active_deal = self.get_active_deal(coin)
        if buy_deals and not sell_deals:
            buy_low = max([x['range'][0] for x in buy_deals])
            buy_top = min([x['range'][1] for x in buy_deals])
            if buy_low > buy_top:
                if active_deal:
                    self.delete_order(buy_deals[0]['ex'], active_deal['order_id'])
                    print(buy_deals)
                return
            buy_px = (buy_low + buy_top) / 2
            if active_deal:
                if active_deal['side'] == 'buy':
                    if buy_low < active_deal['price'] < buy_top:
                        return
                    else:
                        self.amend_order(deal, exchange_sell, active_deal['order_id'])
                else:
                    self.change_order(deal, exchange_sell, active_deal['order_id'])
            else:
                self.new_order(deal, exchange_sell)


if __name__ == '__main__':
    pass
    # from clients_markets_data import coins_symbols_client
    # # from clients-http.kraken import KrakenClient
    # # from clients-http.binance import BinanceClient
    # # from clients-http.dydx import DydxClient
    # # from clients-http.apollox import ApolloxClient
    #
    # clients_list = [DydxClient(), KrakenClient(), BinanceClient(), ApolloxClient()]  # , Bitfinex()]  # ,
    # Bitspay(), Ascendex()]
    # markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    # finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    # data = {}
    # finder.arbitrage(data)

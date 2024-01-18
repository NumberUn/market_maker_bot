import asyncio
from core.wrappers import try_exc_regular, try_exc_async


class MarketFinder:

    def __init__(self, markets, clients_with_names, multibot):
        self.multibot = multibot
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.taker_fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.maker_fees = {x: y.maker_fee for x, y in self.clients_with_names.items()}
        self.mm_exchange = self.multibot.mm_exchange

    def get_active_deal(self, coin):
        if order := self.multibot.open_orders.get(coin):
            return order
        else:
            return []

    @try_exc_regular
    def change_order(self, deal, coin, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.change_maker_order(deal, coin, order_id))

    @try_exc_regular
    def amend_order(self, deal, coin, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.amend_maker_order(deal, coin, order_id))

    @try_exc_regular
    def delete_order(self, coin, order_id):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.delete_maker_order(coin, order_id))

    @try_exc_regular
    def new_order(self, deal, coin):
        loop = asyncio.get_event_loop()
        loop.create_task(self.multibot.new_maker_order(deal, coin))

    @try_exc_regular
    def check_exchanges(self, exchange, exchange_buy, exchange_sell, client_buy, client_sell, coin):
        if exchange not in [exchange_buy, exchange_sell]:
            return False
        if self.mm_exchange not in [exchange_buy, exchange_sell]:
            return False
        if exchange_buy == exchange_sell:
            return False
        if buy_mrkt := client_buy.markets.get(coin):
            if sell_mrkt := client_sell.markets.get(coin):
                return {'buy_mrkt': buy_mrkt, 'sell_mrkt': sell_mrkt}
        return False

    @try_exc_regular
    def check_orderbooks(self, ob_buy, ob_sell):
        if not ob_buy or not ob_sell:
            return False
        if not ob_buy.get('bids') or not ob_buy.get('asks'):
            return False
        if not ob_sell.get('bids') or not ob_sell.get('asks'):
            return False
        return True

    @try_exc_async
    async def count_one_coin(self, coin, exchange):
        buy_deals = []
        sell_deals = []
        for exchange_buy, client_buy in self.clients_with_names.items():
            for exchange_sell, client_sell in self.clients_with_names.items():
                markets = self.check_exchanges(exchange, exchange_buy, exchange_sell, client_buy, client_sell, coin)
                if not markets:
                    continue
                ob_buy = client_buy.get_orderbook(markets['buy_mrkt'])
                ob_sell = client_sell.get_orderbook(markets['sell_mrkt'])
                if not self.check_orderbooks():
                    continue
                # BUY SIDE COUNTINGS
                if exchange_buy == self.mm_exchange:
                    tick = client_buy.instruments[markets['buy_mrkt']]['tick_size']
                    buy_range = [ob_buy['bids'][0][0] + tick, ob_buy['asks'][0][0] - tick]
                    fees = self.maker_fees[exchange_buy] + self.taker_fees[exchange_sell]
                    zero_profit_buy_px = ob_sell['bids'][2][0] * (1 - fees)
                    if zero_profit_buy_px > buy_range[1]:
                        buy_deals.append({'target': ob_sell['bids'][2],
                                          'range': buy_range,
                                          'op_ex': exchange_sell,
                                          'fees': fees})
                    elif buy_range[0] < zero_profit_buy_px < buy_range[1]:
                        buy_range = [buy_range[0], zero_profit_buy_px]
                        buy_deals.append({'target': ob_sell['bids'][2],
                                          'range': buy_range,
                                          'op_ex': exchange_sell,
                                          'fees': fees})
                # SELL SIDE COUNTINGS
                if exchange_sell == self.mm_exchange:
                    tick = client_sell.instruments[markets['sell_mrkt']]['tick_size']
                    sell_range = [ob_sell['asks'][0][0] - tick, ob_sell['bids'][0][0] + tick]
                    fees = self.maker_fees[exchange_sell] + self.taker_fees[exchange_buy]
                    zero_profit_sell_px = ob_buy['asks'][2][0] * (1 + fees)
                    if zero_profit_sell_px < sell_range[1]:
                        sell_deals.append({'target': ob_buy['asks'][2],
                                           'range': sell_range,
                                           'op_ex': exchange_buy,
                                           'fees': fees})
                    elif sell_range[0] > zero_profit_sell_px > sell_range[1]:
                        sell_range = [sell_range[0], zero_profit_sell_px]
                        sell_deals.append({'target': ob_buy['asks'][2],
                                           'range': sell_range,
                                           'op_ex': exchange_buy,
                                           'fees': fees})

        self.choose_maker_order(sell_deals, buy_deals, coin)

    def choose_maker_order(self, sell_deals, buy_deals, coin):
        active_deal = self.get_active_deal(coin)
        buy_deal = None
        sell_deal = None
        if buy_deals:
            buy_low = max([x['range'][0] for x in buy_deals])
            buy_top = min([x['range'][1] for x in buy_deals])
            if buy_low > buy_top: # TODO research if its possible at all
                if active_deal:
                    self.delete_order(coin, active_deal[0])
            else:
                price = (buy_low + buy_top) / 2
                sell_price = buy_deals[0]['target'][0]
                profit = (sell_price - price) / price - buy_deals[0]['fees']
                buy_deal = {'side': 'buy', 'price': price, 'coin': coin,
                            'profit': profit, 'range': [buy_low, buy_top]}
        if sell_deals:
            sell_low = max([x['range'][0] for x in sell_deals])
            sell_top = min([x['range'][1] for x in sell_deals])
            if sell_low < sell_top: # TODO research if its possible at all
                if active_deal:
                    self.delete_order(coin, active_deal[0])
            else:
                price = (sell_low + sell_top) / 2
                buy_price = sell_deals[0]['target'][0]
                profit = (price - buy_price) / buy_price - sell_deals[0]['fees']
                sell_deal = {'side': 'sell', 'price': price, 'coin': coin,
                            'profit': profit, 'range': [sell_low, sell_top]}
        if sell_deal and buy_deal:
            top_deal = sell_deal if sell_deal['profit'] > buy_deal['profit'] else buy_deal
        elif sell_deal and not buy_deal:
            top_deal = sell_deal
        else:
            top_deal = buy_deal
        if active_deal:
            if active_deal[1]['side'] == 'buy':
                if not buy_deal:
                    self.delete_order(coin, active_deal[0])
                    print(f"DELETE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}")
                elif buy_deal['range'][0] < active_deal[1]['price'] < buy_deal['range'][1]:
                    print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                    return
                elif top_deal['side'] == 'buy':
                    self.amend_order(top_deal, coin, active_deal[0])
                    print(f"AMEND\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                elif top_deal['side'] == 'sell':
                    self.change_order(top_deal, coin, active_deal[0])
                    print(f"CHANGE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
            else:
                if not sell_deal:
                    self.delete_order(coin, active_deal[0])
                    print(f"DELETE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                elif sell_deal['range'][0] < active_deal[1]['price'] < sell_deal['range'][1]:
                    print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                    return
                elif top_deal['side'] == 'sell':
                    self.amend_order(top_deal, coin, active_deal[0])
                    print(f"AMEND\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                elif top_deal['side'] == 'buy':
                    self.change_order(top_deal, coin, active_deal[0])
                    print(f"CHANGE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
        else:
            self.new_order(top_deal, coin)
            print(f"CREATE NEW ORDER {coin} {top_deal}\n")


if __name__ == '__main__':
    pass

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
    def check_exchanges(self, exchange, ex_buy, ex_sell, client_buy, client_sell, coin):
        if exchange not in [ex_buy, ex_sell]:
            return False
        if self.mm_exchange not in [ex_buy, ex_sell]:
            return False
        if ex_buy == ex_sell:
            return False
        if buy_mrkt := client_buy.markets.get(coin):
            if sell_mrkt := client_sell.markets.get(coin):
                return {'buy': buy_mrkt, 'sell': sell_mrkt}
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
        active_deal = self.get_active_deal(coin)
        active_deal_price = active_deal[1]['price'] if active_deal else 0
        for ex_buy, client_buy in self.clients_with_names.items():
            for ex_sell, client_sell in self.clients_with_names.items():
                markets = self.check_exchanges(exchange, ex_buy, ex_sell, client_buy, client_sell, coin)
                if not markets:
                    continue
                ob_buy = client_buy.get_orderbook(markets['buy'])
                ob_sell = client_sell.get_orderbook(markets['sell'])
                if not self.check_orderbooks(ob_buy, ob_sell):
                    continue
                # BUY SIDE COUNTINGS
                if ex_buy == self.mm_exchange:
                    tick = client_buy.instruments[markets['buy']]['tick_size']
                    if active_deal_price != ob_buy['bids'][0][0]:
                        best_price = ob_buy['bids'][0][0] + tick
                    else:
                        best_price = ob_buy['bids'][1][0] + tick
                    worst_price = ob_buy['asks'][0][0] - tick
                    if max_size_usd := self.multibot.if_tradable(ex_buy,
                                                                 ex_sell,
                                                                 markets['buy'],
                                                                 markets['sell'],
                                                                 buy_px=best_price,
                                                                 sell_px=ob_sell['bids'][2][0]):
                        max_size_coin = max_size_usd / best_price
                        fees = self.maker_fees[ex_buy] + self.taker_fees[ex_sell]
                        zero_profit_buy_px = ob_sell['bids'][2][0] * (1 - fees)
                        if zero_profit_buy_px >= worst_price:
                            buy_deals.append({'fees': fees,
                                              'op_ex': ex_sell,
                                              'target': ob_sell['bids'][2],
                                              'max_size_coin': max_size_coin,
                                              'range': [best_price, worst_price]})
                        elif best_price <= zero_profit_buy_px <= worst_price:
                            buy_deals.append({'fees': fees,
                                              'op_ex': ex_sell,
                                              'target': ob_sell['bids'][2],
                                              'max_size_coin': max_size_coin,
                                              'range': [best_price, zero_profit_buy_px]})
                # SELL SIDE COUNTINGS
                if ex_sell == self.mm_exchange:
                    tick = client_sell.instruments[markets['sell']]['tick_size']
                    if active_deal_price != ob_sell['asks'][0][0]:
                        best_price = ob_sell['asks'][0][0] - tick
                    else:
                        best_price = ob_sell['asks'][1][0] - tick
                    worst_price = ob_sell['bids'][0][0] + tick
                    if max_size_usd := self.multibot.if_tradable(ex_buy,
                                                                 ex_sell,
                                                                 markets['buy'],
                                                                 markets['sell'],
                                                                 sell_px=best_price,
                                                                 buy_px=ob_buy['asks'][2][0]):
                        max_size_coin = max_size_usd / best_price
                        fees = self.maker_fees[ex_sell] + self.taker_fees[ex_buy]
                        zero_profit_sell_px = ob_buy['asks'][2][0] * (1 + fees)
                        if zero_profit_sell_px <= worst_price:
                            sell_deals.append({'fees': fees,
                                               'op_ex': ex_buy,
                                               'target': ob_buy['asks'][2],
                                               'max_size_coin': max_size_coin,
                                               'range': [best_price, worst_price]})
                        elif best_price >= zero_profit_sell_px >= worst_price:
                            sell_deals.append({'fees': fees,
                                               'op_ex': ex_buy,
                                               'target': ob_buy['asks'][2],
                                               'max_size_coin': max_size_coin,
                                               'range': [best_price, zero_profit_sell_px]})

        if sell_deals or buy_deals:
            # print(f"COUNTINGS FOR {coin}")
            # for deal in sell_deals:
            #     print(f"BUY DEAL: {deal}")
            # for deal in buy_deals:
            #     print(f"SELL DEAL: {deal}")
            self.choose_maker_order(sell_deals, buy_deals, coin)

    def choose_maker_order(self, sell_deals, buy_deals, coin):
        active_deal = self.get_active_deal(coin)
        buy_deal = None
        sell_deal = None
        if buy_deals:
            buy_low = max([x['range'][0] for x in buy_deals])
            buy_top = min([x['range'][1] for x in buy_deals])
            if buy_low > buy_top:  # TODO research if its possible at all
                print(f"{buy_deals=}")
                print(f"buy_low > buy_top!!!!!! CHECK BUY DEALS\n\n\n")
                if active_deal:
                    self.delete_order(coin, active_deal[0])
                    return
            else:
                price = (buy_low + buy_top) / 2
                sell_price = buy_deals[0]['target'][0]
                size = min(buy_deals[0]['max_size_coin'], buy_deals[0]['target'][1])
                profit = (sell_price - price) / price - buy_deals[0]['fees']
                buy_deal = {'side': 'buy', 'price': price, 'size': size, 'coin': coin,
                            'profit': profit, 'range': [buy_low, buy_top]}
        if sell_deals:
            sell_low = max([x['range'][0] for x in sell_deals])
            sell_top = min([x['range'][1] for x in sell_deals])
            if sell_low < sell_top:  # TODO research if its possible at all
                print(f"{sell_deals=}")
                print(f"sell_low < sell_top!!!!!! CHECK SELL DEALS\n\n\n")
                if active_deal:
                    self.delete_order(coin, active_deal[0])
                    return
            else:
                price = (sell_low + sell_top) / 2
                buy_price = sell_deals[0]['target'][0]
                size = min(sell_deals[0]['max_size_coin'], sell_deals[0]['target'][1])
                profit = (price - buy_price) / buy_price - sell_deals[0]['fees']
                sell_deal = {'side': 'sell', 'price': price, 'size': size, 'coin': coin,
                             'profit': profit, 'range': [sell_low, sell_top]}
        if sell_deal and buy_deal:
            top_deal = sell_deal if sell_deal['profit'] > buy_deal['profit'] else buy_deal
        elif sell_deal and not buy_deal:
            top_deal = sell_deal
        else:
            top_deal = buy_deal
        # print(f"TOP DEAL: {top_deal}")
        # print()
        # return
        if active_deal:
            if active_deal[1]['side'] == 'buy':
                if not buy_deal:
                    self.delete_order(coin, active_deal[0])
                    print(f"DELETE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}")
                    return
                elif buy_deal['range'][0] <= active_deal[1]['price'] < buy_deal['range'][1]:
                    print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                    return
                elif top_deal['side'] == 'buy':
                    if coin not in self.multibot.requests_in_progress:
                        self.multibot.requests_in_progress.append(coin)
                        self.amend_order(top_deal, coin, active_deal[0])
                        print(f"AMEND\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                        return
                elif top_deal['side'] == 'sell':
                    self.delete_order(coin, active_deal[0])
                    self.new_order(top_deal, coin)
                    print(f"CHANGE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                    return
            else:
                if not sell_deal:
                    self.delete_order(coin, active_deal[0])
                    print(f"DELETE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                    return
                elif sell_deal['range'][0] <= active_deal[1]['price'] < sell_deal['range'][1]:
                    print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                    return
                elif top_deal['side'] == 'sell':
                    if coin not in self.multibot.requests_in_progress:
                        self.multibot.requests_in_progress.append(coin)
                        self.amend_order(top_deal, coin, active_deal[0])
                        print(f"AMEND\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                        return
                elif top_deal['side'] == 'buy':
                    self.delete_order(coin, active_deal[0])
                    self.new_order(top_deal, coin)
                    print(f"CHANGE\nORDER {coin} {active_deal[1]['side']} EXPIRED. PRICE: {active_deal[1]['price']}\n")
                    return
        else:
            if coin not in self.multibot.requests_in_progress:
                self.multibot.requests_in_progress.append(coin)
                self.new_order(top_deal, coin)
                print(f"CREATE NEW ORDER {coin} {top_deal}\n")


if __name__ == '__main__':
    pass

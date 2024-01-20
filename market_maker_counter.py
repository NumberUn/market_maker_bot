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

    @try_exc_regular
    def get_range_buy_side(self, ob_buy, mrkt, top_bid, client_buy, active_px):
        tick = client_buy.instruments[mrkt['buy']]['tick_size']
        if active_px != top_bid:
            best_px = top_bid + tick
        else:
            best_px = ob_buy['bids'][1][0] + tick
        if best_px == ob_buy['asks'][0][0]:
            best_px = top_bid
            worst_px = best_px
        else:
            worst_px = ob_buy['asks'][0][0] - tick
        return best_px, worst_px

    @try_exc_regular
    def get_range_sell_side(self, ob_sell, mrkt, top_ask, client_sell, active_px):
        tick = client_sell.instruments[mrkt['sell']]['tick_size']
        if active_px != top_ask:
            best_px = top_ask - tick
        else:
            best_px = ob_sell['asks'][1][0] - tick
        if best_px == ob_sell['bids'][0][0]:
            best_px = top_ask
            worst_px = best_px
        else:
            worst_px = ob_sell['bids'][0][0] + tick
        return best_px, worst_px

    @try_exc_async
    async def count_one_coin(self, coin, exchange):
        buy_deals = []
        sell_deals = []
        active_deal = self.get_active_deal(coin)
        active_px = active_deal[1]['price'] if active_deal else 0
        for ex_buy, client_buy in self.clients_with_names.items():
            for ex_sell, client_sell in self.clients_with_names.items():
                mrkt = self.check_exchanges(exchange, ex_buy, ex_sell, client_buy, client_sell, coin)
                if not mrkt:
                    continue
                ob_buy = client_buy.get_orderbook(mrkt['buy'])
                ob_sell = client_sell.get_orderbook(mrkt['sell'])
                if not self.check_orderbooks(ob_buy, ob_sell):
                    continue
                # BUY SIDE COUNTINGS
                if ex_buy == self.mm_exchange:
                    top_bid = ob_buy['bids'][0][0]
                    if max_sz_usd := self.multibot.if_tradable(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], top_bid):
                        best_px, worst_px = self.get_range_buy_side(ob_buy, mrkt, top_bid, client_buy, active_px)
                        max_sz_coin = max_sz_usd / best_px
                        fees = self.maker_fees[ex_buy] + self.taker_fees[ex_sell]
                        zero_profit_buy_px = ob_sell['bids'][2][0] * (1 - fees)
                        pot_deal = {'fees': fees, 'max_sz_coin': max_sz_coin}
                        if zero_profit_buy_px >= worst_px:
                            pot_deal.update({'range': [best_px, worst_px], 'target': ob_sell['bids'][2]})
                            buy_deals.append(pot_deal)
                        elif best_px <= zero_profit_buy_px <= worst_px:
                            pot_deal.update({'range': [best_px, zero_profit_buy_px], 'target': ob_sell['bids'][2]})
                            buy_deals.append(pot_deal)
                # SELL SIDE COUNTINGS
                elif ex_sell == self.mm_exchange:
                    top_ask = ob_sell['asks'][0][0]
                    if max_sz_usd := self.multibot.if_tradable(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], top_ask):
                        best_px, worst_px = self.get_range_sell_side(ob_sell, mrkt, top_ask, client_sell, active_px)
                        max_sz_coin = max_sz_usd / best_px
                        fees = self.maker_fees[ex_sell] + self.taker_fees[ex_buy]
                        zero_profit_sell_px = ob_buy['asks'][2][0] * (1 + fees)
                        pot_deal = {'fees': fees, 'max_sz_coin': max_sz_coin}
                        if zero_profit_sell_px <= worst_px:
                            pot_deal.update({'range': [worst_px, best_px], 'target': ob_buy['asks'][2]})
                            sell_deals.append(pot_deal)
                        elif best_px >= zero_profit_sell_px >= worst_px:
                            pot_deal.update({'range': [zero_profit_sell_px, best_px], 'target': ob_buy['asks'][2]})
                            sell_deals.append(pot_deal)
        # if sell_deals or buy_deals:
            # print(f"COUNTINGS FOR {coin}")
            # for deal in sell_deals:
            #     print(f"BUY DEAL: {deal}")
            # for deal in buy_deals:
            #     print(f"SELL DEAL: {deal}")
        self.process_parse_results(sell_deals, buy_deals, coin, active_deal)

    @try_exc_regular
    def get_top_deal(self, sell_deals, buy_deals, coin, active_deal):
        buy_deal = None
        sell_deal = None
        top_deal = None
        if buy_deals:
            buy_low = max([x['range'][0] for x in buy_deals])
            buy_top = min([x['range'][1] for x in buy_deals])
            if buy_low > buy_top:  # TODO research if its possible at all
                print(f"{buy_deals=}")
                print(f"buy_low > buy_top! CHECK BUY DEALS\n\n\n")
                if active_deal:
                    self.delete_order(coin, active_deal[0])
                    # return
            else:
                price = (buy_low + buy_top) / 2
                sell_price = buy_deals[0]['target'][0]
                size = min(buy_deals[0]['max_sz_coin'], buy_deals[0]['target'][1])
                profit = (sell_price - price) / price - buy_deals[0]['fees']
                buy_deal = {'side': 'buy', 'price': price, 'size': size, 'coin': coin,
                            'profit': profit, 'range': [round(buy_low, 8), round(buy_top, 8)]}
        if sell_deals:
            sell_low = max([x['range'][0] for x in sell_deals])
            sell_top = min([x['range'][1] for x in sell_deals])
            if sell_low > sell_top:  # TODO research if its possible at all
                print(f"{sell_deals=}")
                print(f"sell_low > sell_top! CHECK SELL DEALS\n\n\n")
                if active_deal:
                    self.delete_order(coin, active_deal[0])
                    # return
            else:
                price = (sell_low + sell_top) / 2
                buy_price = sell_deals[0]['target'][0]
                size = min(sell_deals[0]['max_sz_coin'], sell_deals[0]['target'][1])
                profit = (price - buy_price) / buy_price - sell_deals[0]['fees']
                sell_deal = {'side': 'sell', 'price': price, 'size': size, 'coin': coin,
                             'profit': profit, 'range': [round(sell_low, 8), round(sell_top, 8)]}
        if sell_deal and buy_deal:
            top_deal = sell_deal if sell_deal['profit'] > buy_deal['profit'] else buy_deal
        elif sell_deal and not buy_deal:
            top_deal = sell_deal
        elif buy_deal and not sell_deal:
            top_deal = buy_deal
        # if top_deal:
        #     print(f"TOP DEAL: {top_deal}")
        #     print(f"ACTIVE DEAL: {active_deal}")
        #     print()
        return top_deal, buy_deal, sell_deal

    @try_exc_regular
    def process_parse_results(self, sell_deals, buy_deals, coin, active_deal):
        top_deal, buy_deal, sell_deal = self.get_top_deal(sell_deals, buy_deals, coin, active_deal)
        if active_deal:
            if top_deal:
                if top_deal['side'] == active_deal[1]['side']:
                    if top_deal['range'][0] <= active_deal[1]['price'] <= top_deal['range'][1]:
                        print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                    else:
                        if coin in self.multibot.requests_in_progress:
                            print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                            return
                        self.multibot.requests_in_progress.append(coin)
                        self.amend_order(top_deal, coin, active_deal[0])
                        print(f"AMEND\nOLD: {active_deal[1]}\nNEW: {top_deal}\n")
                else:
                    if coin in self.multibot.requests_in_progress:
                        print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                        return
                    self.multibot.requests_in_progress.append(coin)
                    self.delete_order(coin, active_deal[0])
                    self.new_order(top_deal, coin)
                    print(f"CHANGE SIDE\nOLD: {active_deal[1]}\nNEW: {top_deal}\n")
            else:
                self.delete_order(coin, active_deal[0])
                print(f"DELETE\nORDER: {active_deal[0]}")
        else:
            if top_deal:
                if coin in self.multibot.requests_in_progress:
                    print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                    return
                self.multibot.requests_in_progress.append(coin)
                self.new_order(top_deal, coin)
                print(f"CREATE NEW ORDER {coin} {top_deal}\n")

if __name__ == '__main__':
    pass

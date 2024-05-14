import asyncio
from core.wrappers import try_exc_regular, try_exc_async
import time
import json
from datetime import datetime


class MarketFinder:

    def __init__(self, markets, clients_with_names, multibot):
        self.multibot = multibot
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.taker_fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.maker_fees = {x: y.maker_fee for x, y in self.clients_with_names.items()}
        self.mm_exchange = self.multibot.mm_exchange
        self.ob_level = self.multibot.count_ob_level
        self.profit_open = self.multibot.profit_open
        self.profit_close = self.multibot.profit_close
        self.trade_mode = 'low'  # middle
        self.min_size = self.multibot.min_size
        self.write_ranges = False
        self.orders_prints = False
        if self.write_ranges:
            self.profit_ranges = self.unpack_ranges()
            self.target_profits = self.get_all_target_profits()

    @try_exc_regular
    def get_all_target_profits(self):
        directions = self.get_coins_profit_ranges()
        if not directions:
            return dict()
        target_profits = dict()
        for direction in directions.keys():
            exchange_1 = direction.split(':')[1].split('|')[0]
            exchange_2 = direction.split(':')[2].split('|')[0]
            coin = direction.split(':')[-1]
            reversed_direction = f"B:{exchange_2}|S:{exchange_1}|C:{coin}"
            if not directions.get(reversed_direction):
                continue
            direction_one = directions[direction]
            direction_two = directions[reversed_direction]

            if exchange_1 not in (self.clients_with_names.keys()) or exchange_2 not in (self.clients_with_names.keys()):
                continue
            sum_freq_1 = 0
            sum_freq_2 = 0
            if exchange_1 == self.mm_exchange:
                fees = self.maker_fees[exchange_1] + self.taker_fees[exchange_2]
            else:
                fees = self.taker_fees[exchange_1] + self.maker_fees[exchange_2]
            ### Choosing target profit as particular rate of frequency appearing in whole range of profits
            i = 0
            profit_1 = None
            profit_2 = None
            # sum_profit = direction_one['range'][i][0] + direction_two['range'][i][0]
            # print(direction_one['direction'], direction_two['direction'])
            # print(sum_profit - fees)
            # print(sum_profit - fees_1)
            while (direction_one['range'][i][0] + direction_two['range'][i][0]) - fees * 2 >= 0:
                profit_1 = direction_one['range'][i][0]
                profit_2 = direction_two['range'][i][0]
                sum_freq_1 += direction_one['range'][i][1]
                sum_freq_2 += direction_two['range'][i][1]
                i += 1
            if profit_2 != None and profit_1 != None:
                equalizer = 1
                while sum_freq_1 > 100 and sum_freq_1 > 2 * sum_freq_2:
                    profit_1 = direction_one['range'][i - equalizer][0]
                    sum_freq_1 -= direction_one['range'][i - equalizer + 1][1]
                    equalizer += 1
                equalizer = 1
                while sum_freq_2 > 100 and sum_freq_2 > 2 * sum_freq_1:
                    profit_2 = direction_two['range'][i - equalizer][0]
                    sum_freq_2 -= direction_two['range'][i - equalizer + 1][1]
                    equalizer += 1
                # freq_relative_1 = sum_freq_1 / direction_one['range_len'] * 100
                # freq_relative_2 = sum_freq_2 / direction_two['range_len'] * 100
                # print(F"TARGET PROFIT {direction}:", profit_1, sum_freq_1, f"{freq_relative_1} %")
                # print(F"TARGET PROFIT REVERSED {reversed_direction}:", profit_2, sum_freq_2, f"{freq_relative_2} %")
                # print()
                ### Defining of target profit including exchange fees
                target_profits.update({direction: profit_1 - fees,
                                       reversed_direction: profit_2 - fees})
        return target_profits

    @staticmethod
    @try_exc_regular
    def unpack_ranges() -> dict:
        try:
            try:
                with open(f'ranges{str(datetime.now()).split(" ")[0]}.json', 'r') as file:
                    return json.load(file)
            except:
                with open('ranges.json', 'r') as file:
                    return json.load(file)
        except Exception:
            with open('ranges.json', 'w') as file:
                new = {'timestamp': time.time(), 'timestamp_start': time.time()}
                json.dump(new, file)
            return new

    @try_exc_regular
    def get_coins_profit_ranges(self):
        directions = dict()
        for direction in self.profit_ranges.keys():
            if 'timestamp' in direction:
                # Passing the timestamp key in profit_ranges dict
                continue
            range = sorted([[float(x), y] for x, y in self.profit_ranges[direction].items()], reverse=True)
            range_len = sum([x[1] for x in range])
            upd_data = {direction: {'range': range,  # profits dictionary in format key = profit, value = frequency
                                    'range_len': range_len}}
                                # direction in format B:{exch_buy}|S:{exch_sell}|C:{coin} (str)
            directions.update(upd_data)
        return directions

    def get_active_deal(self, coin):
        if order := self.multibot.open_orders.get(coin + '-' + self.multibot.mm_exchange):
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
    def check_orderbooks(self, ob_buy: dict, ob_sell: dict, now_ts: float, active_deal: dict) -> bool:
        if not ob_buy or not ob_sell:
            return False
        if not ob_buy.get('bids') or not ob_buy.get('asks'):
            return False
        if not ob_sell.get('bids') or not ob_sell.get('asks'):
            return False
        if active_deal:
            return True
        # return self.timestamps_filter(ob_buy, ob_sell, now_ts)
        return True

    @try_exc_regular
    def timestamps_filter(self, ob_buy: dict, ob_sell: dict, now_ts: float) -> bool:
        buy_own_ts_ping = now_ts - ob_buy['ts_ms']
        sell_own_ts_ping = now_ts - ob_sell['ts_ms']
        if isinstance(ob_buy['timestamp'], float):
            ts_buy = now_ts - ob_buy['timestamp']
        else:
            ts_buy = now_ts - ob_buy['timestamp'] / 1000
        if isinstance(ob_sell['timestamp'], float):
            ts_sell = now_ts - ob_sell['timestamp']
        else:
            ts_sell = now_ts - ob_sell['timestamp'] / 1000
        if ts_sell > 0.3 or ts_buy > 0.3 or buy_own_ts_ping > 0.060 or sell_own_ts_ping > 0.060:
            return False
        return True

    @try_exc_regular
    def get_range_buy_side(self, ob_buy: dict, mrkt: dict, top_bid: float, client_buy, active_px: float):
        tick = client_buy.instruments[mrkt['buy']]['tick_size']
        if active_px != top_bid:
            best_px = top_bid + tick
        else:
            if len(ob_buy['bids']) > 1:
                best_px = ob_buy['bids'][1][0] + tick
            else:
                best_px = ob_buy['bids'][0][0] - tick
        if best_px == ob_buy['asks'][0][0]:
            best_px = top_bid
            worst_px = best_px
        else:
            worst_px = ob_buy['asks'][0][0] - tick
        return best_px, worst_px, tick

    @try_exc_regular
    def get_range_sell_side(self, ob_sell: dict, mrkt: dict, top_ask: float, client_sell, active_px: float):
        tick = client_sell.instruments[mrkt['sell']]['tick_size']
        if active_px != top_ask:
            best_px = top_ask - tick
        else:
            if len(ob_sell['asks']) > 1:
                best_px = ob_sell['asks'][1][0] - tick
            else:
                best_px = ob_sell['asks'][0][0] + tick
        if best_px == ob_sell['bids'][0][0]:
            best_px = top_ask
            worst_px = best_px
        else:
            worst_px = ob_sell['bids'][0][0] + tick
        return best_px, worst_px, tick

    @try_exc_regular
    def get_deal_direction(self, exchange_buy: str, exchange_sell: str,
                           buy_market: str, sell_market: str, sz_coin: float):
        poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
        buy_close = False
        sell_close = False
        if pos_buy := poses[exchange_buy].get(buy_market):
            buy_close = True if pos_buy['amount_usd'] < 0 else False
        if pos_sell := poses[exchange_sell].get(sell_market):
            sell_close = True if pos_sell['amount_usd'] > 0 else False
        # print("POSES:", poses)
        # print(f'BUY: {exchange_buy} {buy_market} {pos_buy}')
        # print(f'SELL: {exchange_sell} {sell_market} {pos_sell}')
        if buy_close and sell_close and pos_sell['amount'] * 3 > sz_coin:
            sz_coin = pos_sell['amount'] if pos_sell['amount'] < sz_coin else sz_coin
                # print('CLOSE\n')
            return 'close', sz_coin
        else:
            # print('OPEN\n')
            return 'open', sz_coin

    @try_exc_regular
    def count_direction_profit(self, deal_direction):
        if deal_direction == 'open':
            return self.profit_open
        elif deal_direction == 'close':
            return self.profit_close

    @try_exc_regular
    def get_target_profit(self, name, direction):
        target_profit = None
        if self.write_ranges:
            target_profit = self.target_profits.get(name)
        if not target_profit:
            target_profit = self.count_direction_profit(direction)
        return target_profit

    @try_exc_async
    async def count_one_coin(self, coin, exchange):
        buy_deals = []
        sell_deals = []
        active_deal = self.get_active_deal(coin)
        active_px = active_deal[1]['price'] if active_deal else 0
        now_ts = time.time()
        counts = 0
        for ex_buy, client_buy in self.clients_with_names.items():
            for ex_sell, client_sell in self.clients_with_names.items():
                mrkt = self.check_exchanges(exchange, ex_buy, ex_sell, client_buy, client_sell, coin)
                if not mrkt:
                    continue
                ob_buy = client_buy.get_orderbook(mrkt['buy'])
                ob_sell = client_sell.get_orderbook(mrkt['sell'])
                if not self.check_orderbooks(ob_buy, ob_sell, now_ts, active_deal):
                    # print(f"ORDERBOOKS FAILURE: {coin}")
                    continue
                counts += 1
                # BUY SIDE COUNTINGS
                if ex_buy == self.mm_exchange:
                    top_bid = ob_buy['bids'][0][0]
                    # TEST PROFIT RANGES CODE BUY
                    # top_profit = (ob_sell['bids'][self.ob_level][0] - best_px) / best_px - fees
                    # low_profit = (ob_sell['bids'][self.ob_level][0] - worst_px) / worst_px - fees
                    # print(f"{coin} BUY PROFIT RANGE: {round(top_profit, 6)} - {round(low_profit, 6)}")
                    if max_sz_usd := self.multibot.if_tradable(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], top_bid):
                        if min(max_sz_usd, top_bid * ob_sell['bids'][self.ob_level][1]) < self.min_size:
                            continue
                        best_px, worst_px, tick = self.get_range_buy_side(ob_buy, mrkt, top_bid, client_buy, active_px)
                        fees = self.maker_fees[ex_buy] + self.taker_fees[ex_sell]
                        sz_coin = max_sz_usd / best_px
                        direction, sz_coin = self.get_deal_direction(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], sz_coin)
                        if direction == 'open':
                            continue
                        name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        target_profit = self.get_target_profit(name, direction)
                        if target_profit and target_profit < 0 and direction != 'close':
                            continue
                        zero_profit_buy_px = ob_sell['bids'][self.ob_level][0] * (1 - fees - target_profit)
                        pot_deal = {'fees': fees, 'sz_coin': sz_coin, 'direction': direction, 'tick': tick}
                        if zero_profit_buy_px >= worst_px:
                            pot_deal.update({'range': [best_px, worst_px], 'target': ob_sell['bids'][self.ob_level]})
                            buy_deals.append(pot_deal)
                        elif best_px <= zero_profit_buy_px <= worst_px:
                            pot_deal.update({'range': [best_px, zero_profit_buy_px], 'target': ob_sell['bids'][self.ob_level]})
                            buy_deals.append(pot_deal)
                # SELL SIDE COUNTINGS
                elif ex_sell == self.mm_exchange:
                    top_ask = ob_sell['asks'][0][0]
                    # TEST PROFIT RANGES CODE SELL
                    # top_profit = (best_px - ob_buy['asks'][self.ob_level][0]) / ob_buy['asks'][self.ob_level][0] - fees
                    # low_profit = (worst_px - ob_buy['asks'][self.ob_level][0]) / ob_buy['asks'][self.ob_level][0] - fees
                    # print(f"{coin} SELL PROFIT RANGE: {round(top_profit, 6)} - {round(low_profit, 6)}")
                    if max_sz_usd := self.multibot.if_tradable(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], top_ask):
                        if min(max_sz_usd, top_ask * ob_buy['asks'][self.ob_level][1]) < self.min_size:
                            continue
                        best_px, worst_px, tick = self.get_range_sell_side(ob_sell, mrkt, top_ask, client_sell, active_px)
                        fees = self.maker_fees[ex_sell] + self.taker_fees[ex_buy]
                        sz_coin = max_sz_usd / best_px
                        direction, sz_coin = self.get_deal_direction(ex_buy, ex_sell, mrkt['buy'], mrkt['sell'], sz_coin)
                        if direction == 'open':
                            continue
                        name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        target_profit = self.get_target_profit(name, direction)
                        if target_profit and target_profit < 0 and direction != 'close':
                            continue
                        zero_profit_sell_px = ob_buy['asks'][self.ob_level][0] * (1 + fees + target_profit)
                        pot_deal = {'fees': fees, 'sz_coin': sz_coin, 'direction': direction, 'tick': tick}
                        if zero_profit_sell_px <= worst_px:
                            pot_deal.update({'range': [worst_px, best_px], 'target': ob_buy['asks'][self.ob_level]})
                            sell_deals.append(pot_deal)
                        elif best_px >= zero_profit_sell_px >= worst_px:
                            pot_deal.update({'range': [zero_profit_sell_px, best_px], 'target': ob_buy['asks'][self.ob_level]})
                            sell_deals.append(pot_deal)
        # if sell_deals or buy_deals:
        #     print(f"COUNTINGS FOR {coin}")
        #     for deal in sell_deals:
        #         print(f"BUY DEAL: {deal}")
        #     for deal in buy_deals:
        #         print(f"SELL DEAL: {deal}")
        #     print('\n')
        if counts:
            self.process_parse_results(sell_deals, buy_deals, coin, active_deal, now_ts)

    @try_exc_regular
    def get_top_deal(self, sell_deals, buy_deals, coin, active_deal, now_ts):
        buy_deal = None
        sell_deal = None
        top_deal = None
        if buy_deals:
            buy_low = min([x['range'][0] for x in buy_deals])
            buy_top = max([x['range'][1] for x in buy_deals])
            if self.trade_mode == 'low':
                price = buy_top
            elif self.trade_mode == 'middle':
                price = (buy_low + buy_top) / 2
            sell_price = buy_deals[0]['target'][0]
            size = min(buy_deals[0]['sz_coin'], buy_deals[0]['target'][1])
            profit = (sell_price - price) / price - buy_deals[0]['fees']
            buy_deal = {'side': 'buy', 'price': price, 'size': size, 'coin': coin, 'last_update': now_ts,
                        'profit': profit, 'range': [round(buy_low, 8), round(buy_top, 8)], 'target': sell_price,
                        'direction': buy_deals[0]['direction'], 'tick': buy_deals[0]['tick']}
        if sell_deals:
            sell_low = min([x['range'][0] for x in sell_deals])
            sell_top = max([x['range'][1] for x in sell_deals])
            if self.trade_mode == 'low':
                price = sell_low
            elif self.trade_mode == 'middle':
                price = (sell_low + sell_top) / 2
            buy_price = sell_deals[0]['target'][0]
            size = min(sell_deals[0]['sz_coin'], sell_deals[0]['target'][1])
            profit = (price - buy_price) / buy_price - sell_deals[0]['fees']
            sell_deal = {'side': 'sell', 'price': price, 'size': size, 'coin': coin, 'last_update': now_ts,
                         'profit': profit, 'range': [round(sell_low, 8), round(sell_top, 8)], 'target': buy_price,
                         'direction': sell_deals[0]['direction'], 'tick': sell_deals[0]['tick']}
        if sell_deal and buy_deal:
            if active_deal:
                top_deal = sell_deal if active_deal[1]['side'] == 'sell' else buy_deal
            else:
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
    def process_parse_results(self, sell_deals, buy_deals, coin, active_deal, now_ts):
        top_deal, buy_deal, sell_deal = self.get_top_deal(sell_deals, buy_deals, coin, active_deal, now_ts)
        market_id = coin + '-' + self.multibot.mm_exchange
        if active_deal:
            if top_deal:
                if top_deal['side'] == active_deal[1]['side']:
                    tick = top_deal['tick']
                    if top_deal['range'][0] - tick < active_deal[1]['price'] < top_deal['range'][1] + tick:
                        if active_deal[1]['size'] <= top_deal['size']:
                            if self.multibot.requests_in_progress.get(market_id):
                                if self.orders_prints:
                                    print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                                return
                            self.multibot.open_orders[market_id][1].update({'last_update': now_ts})
                            if self.orders_prints:
                                print(f"ORDER {coin} {active_deal[1]['side']} STILL GOOD. PRICE: {active_deal[1]['price']}\n")
                        else:
                            if self.multibot.requests_in_progress.get(market_id):
                                if self.orders_prints:
                                    print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                                return
                            self.multibot.requests_in_progress.update({market_id: True})
                            self.amend_order(top_deal, coin, active_deal[0])
                    else:
                        if self.multibot.requests_in_progress.get(market_id):
                            if self.orders_prints:
                                print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                            return
                        self.multibot.requests_in_progress.update({market_id: True})
                        self.amend_order(top_deal, coin, active_deal[0])
                        if self.orders_prints:
                            print(f"AMEND\nOLD: {active_deal[1]}\nNEW: {top_deal}\n")
                else:
                    if self.multibot.requests_in_progress.get(market_id):
                        if self.orders_prints:
                            print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                        return
                    self.multibot.requests_in_progress.update({market_id: True})
                    self.delete_order(coin, active_deal[0])
                # else:
                #     if self.multibot.requests_in_progress.get(market_id):
                #         # print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                #         return
                #     self.multibot.requests_in_progress.update({market_id: True})
                #     self.delete_order(coin, active_deal[0])
                #     while self.multibot.requests_in_progress.get(market_id):
                #         # print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                #         time.sleep(0.001)
                #     self.new_order(top_deal, coin)
                #     # print(f"CHANGE SIDE\nOLD: {active_deal[1]}\nNEW: {top_deal}\n")
            else:
                if self.multibot.requests_in_progress.get(market_id):
                    if self.orders_prints:
                        print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                    return
                self.multibot.requests_in_progress.update({market_id: True})
                self.delete_order(coin, active_deal[0])
                if self.orders_prints:
                    print(f"DELETE\nORDER: {active_deal}")
        else:
            if top_deal:
                if self.multibot.requests_in_progress.get(market_id):
                    if self.orders_prints:
                        print(f"{coin} REQUEST IS IN PROGRESS. BREAK")
                    return
                self.multibot.requests_in_progress.update({market_id: True})
                self.new_order(top_deal, coin)
                if self.orders_prints:
                    print(f"CREATE NEW ORDER {coin} {top_deal}\n")


if __name__ == '__main__':
    pass

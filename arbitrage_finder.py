import asyncio
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async
import time
import json
import traceback
from core.ap_class import AP
import gc
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

try:
    with open('ranges.json', 'r') as f:
        pass
except Exception as e:
    # traceback.print_exc()
    with open('ranges.json', 'w') as f:
        pass


class ArbitrageFinder:

    def __init__(self, multibot, markets, clients_with_names, profit_taker, profit_close, state='Bot'):
        self.multibot = multibot
        self.write_ranges = False
        self.state = state
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        if self.write_ranges:
            self.last_record = time.time()
            self.profit_precise = 4
            self.profit_ranges = self.unpack_ranges()
            self.target_profits = self.get_all_target_profits()
            print(f"TARGET PROFIT RANGES FOR {(time.time() - self.profit_ranges['timestamp_start']) / 3600} HOURS")
            print(self.target_profits)

    @try_exc_regular
    def get_target_profit(self, deal_direction):
        if deal_direction == 'open':
            target_profit = self.profit_taker
        elif deal_direction == 'close':
            target_profit = self.profit_close
        else:
            target_profit = (self.profit_taker + self.profit_close) / 2
        return target_profit

    @try_exc_regular
    def get_deal_direction(self, positions, exchange_buy, exchange_sell, buy_market, sell_market):
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

    def target_profit_exceptions(self, data):
        targets = dict()
        for coin in self.coins:
            for ex_buy, client_1 in self.clients_with_names.items():
                for ex_sell, client_2 in self.clients_with_names.items():
                    if ex_buy == ex_sell:
                        continue
                    if ob_1 := data.get(ex_buy + '__' + coin):
                        if ob_2 := data.get(ex_sell + '__' + coin):
                            if not ob_2['top_bid'] or not ob_1['top_ask']:
                                continue
                            buy_mrkt = self.markets[coin][ex_buy]
                            sell_mrkt = self.markets[coin][ex_sell]
                            buy_ticksize_rel = client_1.instruments[buy_mrkt]['tick_size'] / ob_1['top_bid']
                            sell_ticksize_rel = client_2.instruments[sell_mrkt]['tick_size'] / ob_2['top_ask']
                            if buy_ticksize_rel > self.profit_taker or sell_ticksize_rel > self.profit_taker:
                                target_profit = 1.5 * max(buy_ticksize_rel, sell_ticksize_rel)
                                targets.update({sell_mrkt + buy_mrkt: target_profit,
                                                buy_mrkt + sell_mrkt: target_profit})
        self.excepts = targets

    @try_exc_regular
    def check_timestamps(self, client_buy, client_sell, ts_buy, ts_sell):
        # buy_own_ts_ping = now_ts - ob_buy['ts_ms']
        # sell_own_ts_ping = now_ts - ob_sell['ts_ms']
        if ts_sell > client_sell.top_ws_ping or ts_buy > client_buy.top_ws_ping:
            return False
        return True

    @try_exc_regular
    def get_ob_pings(self, ob_buy, ob_sell):
        if isinstance(ob_buy['timestamp'], float):
            ts_buy = ob_buy['ts_ms'] - ob_buy['timestamp']
        else:
            ts_buy = ob_buy['ts_ms'] - ob_buy['timestamp'] / 1000
        if isinstance(ob_sell['timestamp'], float):
            ts_sell = ob_sell['ts_ms'] - ob_sell['timestamp']
        else:
            ts_sell = ob_sell['ts_ms'] - ob_sell['timestamp'] / 1000
        return ts_buy, ts_sell

        # is_buy_ping_faster = ts_sell - sell_own_ts_ping > ts_buy - buy_own_ts_ping
        # is_buy_last_ob_update = sell_own_ts_ping > buy_own_ts_ping
        # if is_buy_ping_faster == is_buy_last_ob_update:

    @try_exc_async
    async def count_one_coin(self, coin, trigger_exchange, trigger_side, trigger_type):
        if self.multibot.arbitrage_processing:
            return
        now_ts = time.time()
        for exchange, client in self.clients_with_names.items():
            if trigger_exchange == exchange:
                continue
            if trigger_side == 'buy':
                client_buy = self.clients_with_names[trigger_exchange]
                client_sell = client
                ex_buy = trigger_exchange
                ex_sell = exchange
            else:
                client_buy = client
                client_sell = self.clients_with_names[trigger_exchange]
                ex_buy = exchange
                ex_sell = trigger_exchange
            if buy_mrkt := client_buy.markets.get(coin):
                if sell_mrkt := client_sell.markets.get(coin):
                    ob_buy = client_buy.get_orderbook(buy_mrkt)
                    ob_sell = client_sell.get_orderbook(sell_mrkt)
                    if not ob_buy or not ob_sell:
                        continue
                    if not ob_buy.get('bids') or not ob_buy.get('asks'):
                        continue
                    if not ob_sell.get('bids') or not ob_sell.get('asks'):
                        continue
                    # if not self.check_timestamps(client_buy, client_sell, ts_buy, ts_sell):
                    #     continue
                    poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
                    direction = self.get_deal_direction(poses, ex_buy, ex_sell, buy_mrkt, sell_mrkt)
                    buy_px = ob_buy['asks'][0][0]
                    sell_px = ob_sell['bids'][0][0]
                    raw_profit = (sell_px - buy_px) / buy_px
                    profit = raw_profit - self.fees[ex_buy] - self.fees[ex_sell]
                    name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                    print(f"{name} | Profit: {profit}")
                    if self.write_ranges:
                        name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        self.append_profit(profit=raw_profit, name=name)
                        target_profit = self.target_profits.get(name, 'Not found')
                        if target_profit != 'Not found' and target_profit < 0 and direction == 'open':
                            continue
                        if target_profit == 'Not found':
                            target_profit = self.get_target_profit(direction)
                    else:
                        target_profit = self.get_target_profit(direction)

                        # if buy_trade := client_buy.public_trades.get(buy_mrkt):
                        #     if abs(buy_trade['ts'] - ob_buy['timestamp']) < 0.01:
                        #         print(f'LAST TRADE AND ORDERBOOK ON THE MOMENT: {buy_trade}')
                        #         print(f'ACTUAL OB {ob_buy}')
                        #         print()
                        # elif sell_trade := client_sell.public_trades.get(sell_mrkt):
                        #     if abs(sell_trade['ts'] - ob_sell['timestamp']) < 0.01:
                        #         print(f"TRIGGER: {trigger_exchange}\n{name}\nPROFIT {profit}")
                        #         print(f'LAST TRADE AND ORDERBOOK ON THE MOMENT: {sell_trade}')
                        #         print(f'ACTUAL OB {ob_sell}')
                        #         print()
                    # profit = raw_profit - fees
                    if profit >= target_profit:
                        if gc.isenabled():
                            gc.disable()
                        # name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        print(f"TRIGGER: {trigger_exchange} {trigger_type} {name} PROFIT {profit}")
                        print(f"BUY PX: {buy_px} | SELL PX: {sell_px} | DIRECTION: {direction}")
                        print()
                        ts_buy, ts_sell = self.get_ob_pings(ob_buy, ob_sell)
                        if ts_buy > 0.15 or ts_sell > 0.15:
                            print(f"OB PING IS HUGE: {ts_sell=} {ts_buy=}")
                            print()
                            return
                        deal = {'client_buy': client_buy,
                                'client_sell': client_sell,
                                'buy_px': buy_px,
                                'sell_px': sell_px,
                                'buy_sz': ob_buy['asks'][0][1],
                                'sell_sz': ob_sell['bids'][0][1],
                                'buy_mrkt': buy_mrkt,
                                'sell_mrkt': sell_mrkt,
                                'ts_start_counting': now_ts,
                                'ob_buy_own_ts': ob_buy['ts_ms'],
                                'ob_sell_own_ts': ob_sell['ts_ms'],
                                'ob_buy_api_ts': ts_buy,
                                'ob_sell_api_ts': ts_sell,
                                'ex_buy': ex_buy,
                                'ex_sell': ex_sell,
                                'coin': coin,
                                'target_profit': target_profit,
                                'profit': profit,
                                'direction': direction,
                                'trigger_ex': trigger_exchange,
                                'trigger_type': trigger_type}
                        await self.multibot.run_arbitrage(deal)

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
    def get_all_target_profits(self):
        coins = self.get_coins_profit_ranges()
        if not coins:
            return dict()
        target_profits = dict()
        for coin in coins.keys():
            if 'reversed' in coin:
                continue
            if not coins.get(coin + '_reversed'):
                continue
            direction_one = coins[coin]
            direction_two = coins[coin + '_reversed']
            exchange_1 = direction_one['direction'].split(':')[1].split('|')[0]
            exchange_2 = direction_two['direction'].split(':')[1].split('|')[0]
            if exchange_1 not in (self.clients_with_names.keys()) or exchange_2 not in (self.clients_with_names.keys()):
                continue
            sum_freq_1 = 0
            sum_freq_2 = 0
            fees = self.fees[exchange_1] + self.fees[exchange_2]
            ### Choosing target profit as particular rate of frequency appearing in whole range of profits
            i = 0
            profit_1 = None
            profit_2 = None
            # sum_profit = direction_one['range'][i][0] + direction_two['range'][i][0]
            # print(direction_one['direction'], direction_two['direction'])
            # print(sum_profit - fees)
            # print(sum_profit - fees_1)
            while (direction_one['range'][i][0] + direction_two['range'][i][0]) - 2 * fees >= self.profit_taker:
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
                freq_relative_1 = sum_freq_1 / direction_one['range_len'] * 100
                freq_relative_2 = sum_freq_2 / direction_two['range_len'] * 100
                print(F"TARGET PROFIT {direction_one['direction']}:", profit_1, sum_freq_1, f"{freq_relative_1} %")
                print(F"TARGET PROFIT REVERSED {direction_two['direction']}:", profit_2, sum_freq_2,
                      f"{freq_relative_2} %")
                print()
                ### Defining of target profit including exchange fees
                target_profits.update({direction_one['direction']: profit_1 - fees,
                                       direction_two['direction']: profit_2 - fees})
        return target_profits

    @try_exc_regular
    def append_profit(self, profit: float, name: str):
        profit = round(profit, self.profit_precise)
        if self.profit_ranges.get(name):
            if self.profit_ranges[name].get(profit):
                self.profit_ranges[name][profit] += 1
            else:
                self.profit_ranges[name].update({profit: 1})
        else:
            self.profit_ranges.update({name: {profit: 1}})
        now = time.time()
        self.profit_ranges.update({'timestamp': now})
        if now - self.last_record > 3600:
            with open('ranges.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.last_record = now
        if now - self.profit_ranges['timestamp_start'] > 3600 * 24:
            with open(f'ranges{str(datetime.now()).split(" ")[0]}.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.profit_ranges = {'timestamp': now, 'timestamp_start': now}

    @try_exc_regular
    def get_coins_profit_ranges(self):
        coins = dict()
        for direction in self.profit_ranges.keys():
            if 'timestamp' in direction:
                # Passing the timestamp key in profit_ranges dict
                continue
            coin = direction.split('C:')[1]
            range = sorted([[float(x), y] for x, y in self.profit_ranges[direction].items()], reverse=True)
            range_len = sum([x[1] for x in range])
            if coins.get(coin):
                # Filling reversed direction of trades if one direction for this coin already filled
                coin = coin + '_reversed'
            upd_data = {coin: {'range': range,  # profits dictionary in format key = profit, value = frequency
                               'range_len': range_len,  # sample total size of all records
                               'direction': direction}}  # direction in format B:{exch_buy}|S:{exch_sell}|C:{coin} (str)
            coins.update(upd_data)
        return coins


if __name__ == '__main__':
    pass


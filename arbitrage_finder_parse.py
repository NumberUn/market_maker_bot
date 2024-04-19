import asyncio
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async
import time
import json
import traceback
from core.ap_class import AP
import gc
import uvloop
from core.telegram import Telegram, TG_Groups
import csv

try:
    with open('arbitrage_possibilities.csv', 'r') as file:
        pass
except:
    with open('arbitrage_possibilities.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['ENV', 'BUY_PX', 'SELL_PX', 'BUY_SZ', 'SELL_SZ', 'BUY_MRKT', 'SELL_MRKT',
                         'TS_START_COUNTING', 'OB_BUY_OWN_TS', 'OB_SELL_OWN_TS', 'EX_BUY', 'EX_SELL',
                         'COIN', 'TARGET_PROFIT', 'EXPECT_PROFIT', 'TRIGGER_EX', 'TRIGGER_TYPE'])


telegram = Telegram()

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class ArbitrageFinderParse:

    def __init__(self, markets, clients_with_names, profit_taker, profit_close):
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}

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
            ts_buy = ob_buy['ts_ms'] - (ob_buy['timestamp'] / 1000)

        if isinstance(ob_sell['timestamp'], float):
            ts_sell = ob_sell['ts_ms'] - ob_sell['timestamp']
        else:
            ts_sell = ob_sell['ts_ms'] - (ob_sell['timestamp'] / 1000)
        return ts_buy, ts_sell

    @try_exc_regular
    def get_ob_ages(self, now_ts: float, ob_buy: dict, ob_sell: dict):
        age_buy = now_ts - ob_buy['ts_ms']
        age_sell = now_ts - ob_sell['ts_ms']
        return age_buy, age_sell

        # is_buy_ping_faster = ts_sell - sell_own_ts_ping > ts_buy - buy_own_ts_ping
        # is_buy_last_ob_update = sell_own_ts_ping > buy_own_ts_ping
        # if is_buy_ping_faster == is_buy_last_ob_update:

    @try_exc_async
    async def count_one_coin(self, coin, trigger_exchange, trigger_side, trigger_type):
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
                    if ob_buy:
                        ob_sell = client_sell.get_orderbook(sell_mrkt)
                        if ob_sell:
                            if not ob_buy.get('bids') or not ob_buy.get('asks'):
                                continue
                            if not ob_sell.get('bids') or not ob_sell.get('asks'):
                                continue
                            # if not self.check_timestamps(client_buy, client_sell, ts_buy, ts_sell):
                            #     continue
                            buy_px = ob_buy['asks'][0][0]
                            sell_px = ob_sell['bids'][0][0]
                            raw_profit = (sell_px - buy_px) / buy_px
                            profit = raw_profit - self.fees[ex_buy] - self.fees[ex_sell]
                            # name = f"T:{trigger_exchange}\nB:{ex_buy}|S:{ex_sell}|C:{coin}"
                            # print(f"{name} | Profit: {profit}|TSB:{ts_buy}|TSS:{ts_sell}\n")
                            target_profit = self.get_target_profit('open')

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
                                if trigger_exchange == 'BITKUB':
                                    name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                                    print(f"TRIGGER: {trigger_exchange} {trigger_type} {name} PROFIT {profit}")
                                    print(f"BUY PX: {buy_px} | SELL PX: {sell_px}")
                                    print()

                                    # print(f"OB PING IS HUGE: {ts_sell=} {ts_buy=}")
                                    # print()
                                    # if self.check_spread(ob_buy, 'asks', target_profit):
                                    #     if self.check_spread(ob_sell, 'bids', target_profit):
                                thb_rate = 0
                                if client_buy.EXCHANGE_NAME == 'BITKUB':
                                    thb_rate = client_buy.get_thb_rate()
                                elif client_sell.EXCHANGE_NAME == 'BITKUB':
                                    thb_rate = client_sell.get_thb_rate()
                                deal = {
                                    # 'client_buy': client_buy,
                                    # 'client_sell': client_sell,
                                    'buy_px': buy_px,
                                    'sell_px': sell_px,
                                    'buy_sz': ob_buy['asks'][0][1],
                                    'sell_sz': ob_sell['bids'][0][1],
                                    'buy_mrkt': buy_mrkt,
                                    'sell_mrkt': sell_mrkt,
                                    'ts_start_counting': now_ts,
                                    'ob_buy_own_ts': ob_buy['ts_ms'],
                                    'ob_sell_own_ts': ob_sell['ts_ms'],
                                    # 'ob_buy_api_ts': ts_buy,
                                    # 'ob_sell_api_ts': ts_sell,
                                    'ex_buy': ex_buy,
                                    'ex_sell': ex_sell,
                                    'coin': coin,
                                    'target_profit': target_profit,
                                    'profit': profit,
                                    'trigger_ex': trigger_exchange,
                                    'trigger_type': trigger_type,
                                    'thb_rate': thb_rate}
                                new_line = list(deal.values())
                                new_line.insert(0, 'MULTIBOT_TEST_TOKYO')
                                with open('arbitrage_possibilities.csv', 'a', newline='') as file_to_append:
                                    writer_inside = csv.writer(file_to_append)
                                    # Write a single row
                                    writer_inside.writerow(new_line)
                                # message = '\n'.join([x.upper() + ': ' + str(y) for x, y in deal.items()])
                                # telegram.send_message(message, TG_Groups.MainGroup)

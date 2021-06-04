import math
import threading
import time

import requests
import unicorn_binance_rest_api
import unicorn_binance_websocket_api
from unicorn_fy import UnicornFy

from gorman import logger

log = logger(__name__)

binance_api_key = ""
binance_api_secret = ""


class BinanceStreamer:
    def __init__(self, publisher):
        self.publisher = publisher
        self.channels = {'aggTrade', 'trade', 'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_2h',
                         'kline_4h',
                         'kline_6h', 'kline_8h', 'kline_12h', 'kline_1d', 'kline_3d', 'kline_1w', 'kline_1M',
                         'miniTicker',
                         'ticker', 'bookTicker', 'depth5', 'depth10', 'depth20', 'depth', 'depth@100ms'}
        self.arr_channels = {'!miniTicker', '!ticker', '!bookTicker'}

        try:
            self.binance_rest_client = unicorn_binance_rest_api.BinanceRestApiManager(binance_api_key,
                                                                                      binance_api_secret)
        except requests.exceptions.ConnectionError:
            raise Exception("Network error")

    def print_stream_data_from_stream_buffer(self, binance_websocket_api_manager):
        while True:
            if binance_websocket_api_manager.is_manager_stopping():
                exit(0)
            oldest_stream_data_from_stream_buffer = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
            if oldest_stream_data_from_stream_buffer is not False:
                # print(oldest_stream_data_from_stream_buffer)
                # publisher.send_message('example.text', oldest_stream_data_from_stream_buffer)
                # print(UnicornFy.binance_com_websocket(oldest_stream_data_from_stream_buffer))
               # self.publisher.send_message(
               #     )
                payload = UnicornFy.binance_com_websocket(oldest_stream_data_from_stream_buffer)
                #print("Foo")
                pass
            else:
                time.sleep(0.01)

    def work(self):
        self.binance_websocket_api_manager = unicorn_binance_websocket_api.BinanceWebSocketApiManager()

        # start a worker process to move the received stream_data from the stream_buffer to a print function
        worker_thread = threading.Thread(target=self.print_stream_data_from_stream_buffer,
                                         args=(self.binance_websocket_api_manager,))
        worker_thread.start()

        markets = []
        data = self.binance_rest_client.get_all_tickers()
        for item in data:
            markets.append(item['symbol'])

        # private_stream_id_alice = binance_websocket_api_manager.create_stream(["!userData"],
        #                                                                       ["arr"],
        #                                                                       api_key=binance_api_key,
        #                                                                       api_secret=binance_api_secret,
        #                                                                       stream_label="userData Alice")
        #
        # private_stream_id_bob = binance_websocket_api_manager.create_stream(["!userData"],
        #                                                                     ["arr"],
        #                                                                     api_key="aaa",
        #                                                                     api_secret="bbb",
        #                                                                     stream_label="userData Bob")

        arr_stream_id = self.binance_websocket_api_manager.create_stream(self.arr_channels, "arr",
                                                                         stream_label="arr channels",
                                                                         ping_interval=10, ping_timeout=10,
                                                                         close_timeout=5)

        divisor = math.ceil(
            len(markets) / self.binance_websocket_api_manager.get_limit_of_subscriptions_per_stream())
        max_subscriptions = math.ceil(len(markets) / divisor)

        for channel in self.channels:
            if len(markets) <= max_subscriptions:
                self.binance_websocket_api_manager.create_stream(channel, markets, stream_label=channel)
            else:
                loops = 1
                i = 1
                markets_sub = []
                for market in markets:
                    markets_sub.append(market)
                    if i == max_subscriptions or loops * max_subscriptions + i == len(markets):
                        self.binance_websocket_api_manager.create_stream(channel, markets_sub,
                                                                         stream_label=str(channel + "_" + str(i)),
                                                                         ping_interval=10, ping_timeout=10,
                                                                         close_timeout=5)
                        markets_sub = []
                        i = 1
                        loops += 1
                    i += 1
        return worker_thread

    def stop(self):
        self.binance_websocket_api_manager.stop_manager_with_all_streams()

# time.sleep(10)
# print("Publisher stopping")
# publisher.stop()
# print("Stop manager")
# binance_websocket_api_manager.stop_manager_with_all_streams()
# while True:
#    print("Hei")
# binance_websocket_api_manager.print_summary()
# binance_websocket_api_manager.print_stream_info(arr_stream_id)
#    time.sleep(1)

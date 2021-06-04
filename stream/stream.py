import threading
import time

from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


def print_stream_data_from_stream_buffer(binance_websocket_api_manager):
    while True:
        if binance_websocket_api_manager.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer is False:
            time.sleep(0.01)


def futures():
    future_manager = BinanceWebSocketApiManager(exchange="binance.com-futures")
    forceorder_all_stream_id = future_manager.create_stream(["arr"], ["!forceOrder"])
    worker_thread = threading.Thread(target=print_stream_data_from_stream_buffer, args=(future_manager,))
    worker_thread.start()

    while True:
        # binance_websocket_api_manager.print_summary()
        # time.sleep(1)
        oldest_stream_data_from_stream_buffer = future_manager.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer:
            print(oldest_stream_data_from_stream_buffer)

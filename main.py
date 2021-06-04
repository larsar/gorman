import signal
import sys

from gorman.mq.publisher import Publisher
from gorman.streamer.binance_streamer import BinanceStreamer

# from streamer.stream import futures

# print("hei")
# futures()
# print("bar")





def main():
    publisher = Publisher(
        'amqp://guest:guest@localhost:7201/%2F?connection_attempts=3&heartbeat=3600', 'binance'
    )
    binance_streamer = BinanceStreamer(publisher)

    def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        publisher.stop()
        binance_streamer.stop()
        # if publisher:
        #    publisher.stop()
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    threads_arr = []
    threads_arr.append(publisher)
    publisher.daemon = True # die when the main thread dies
    publisher.start()
    streamer_thread = binance_streamer.work()
    threads_arr.append(streamer_thread)
    for thr in threads_arr: # let them all start before joining
        thr.join()


if __name__ == "__main__":
    main()



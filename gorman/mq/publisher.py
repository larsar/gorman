import logging
import os
import pika
from pika.exchange_type import ExchangeType
import json
from gorman import logger
log = logger(__name__)

import threading


class Publisher(threading.Thread):
    EXCHANGE_TYPE = ExchangeType.topic

    def __init__(self, amqp_url, exchange):
        super(Publisher, self).__init__()
        self.exchange = exchange
        self._connection = None
        self._channel = None

        self._stopping = False
        self._url = amqp_url

    def connect(self):
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        log.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        log.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            log.warning('Connection closed, reopening in 5 seconds: %s',
                           reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        log.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        log.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        log.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        log.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        log.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE)

    def send_message(self, routing_key, type, payload):
        if self._channel is None or not self._channel.is_open:
            return
        self._channel.basic_publish(exchange=self.exchange,
                                    routing_key=routing_key,
                                    properties=pika.BasicProperties(
                                        content_type='application/json',
                                        delivery_mode=1,
                                        expiration=str(60000),
                                        type = type
                                    ),
                                    body=json.dumps(payload).encode('utf-8'))

    def run(self):
        """Run the example code by connecting and then starting the IOLoop.

        """
        while not self._stopping:
            self._connection = None
            self._connection = self.connect()
            self._connection.ioloop.start()

        log.info('Stopped')

    def stop(self):
        log.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None:
            log.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            log.info('Closing connection')
            self._connection.close()

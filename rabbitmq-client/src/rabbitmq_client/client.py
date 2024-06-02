# client.py ----------------------------------------------------------------------------------------
#
# Description:
#    This script contains the client class
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import time
import logging
from typing import Union, List
# Installed
import pika
from pika.credentials import PlainCredentials
# Custom
# NOTE: Add here all the Custom modules


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Classes
# ==================================================================================================
#
class RabbitMQClient:

    def __init__(self, host, port=5672, username=None, password=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        self.connected = False

    # -------------------------------------
    # Connections
    # -------------------------------------
    def connect(self):
        """Connects to the RabbitMQ server, retrying on failure"""
        max_retries, attempts = 3, 0
        while not self.connected and attempts < max_retries:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host,
                                              port=self.port,
                                              credentials=PlainCredentials(self.username, self.password)))
                self.channel = self.connection.channel()
                self.connected = True
                break
            except pika.exceptions.ConnectionClosed:
                attempts += 1
                if attempts < max_retries:
                    logger.info(f"Connection attempt failed. Retrying in 1 second ({attempts}/{max_retries})")
                    time.sleep(1)
                else:
                    raise ConnectionError("Failed to connect to RabbitMQ after all retries")

    def disconnect(self):
        """Disconnects from the RabbitMQ server"""
        if self.is_connected():
            self.channel.close()
            self.connection.close()
            self.connected = False

    def is_connected(self):
        """Checks if connected to the server"""
        return self.connected

    def qos(self, prefetch_count):
        """Changes the Quality Of Service of the channel"""
        # NOTE: For fair dispatch in case of Round-Robin, set prefetch_count = 1
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def run(self):
        """Starts consuming messages from subscribed queues"""
        if self.is_connected():
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                self.channel.stop_consuming()

    def stop(self):
        """Stops consuming messages from subscribed queues"""
        if self.is_connected():
            self.channel.stop_consuming()

    def terminate(self):
        """Steps and disconnects this client"""
        self.stop()
        self.disconnect()

    # -------------------------------------
    # Messaging
    # -------------------------------------
    #
    # Sent Messages
    #
    @staticmethod
    def sent_to(channel, queue: str, message: str, durable: bool = False, **kwargs):
        """Delivers messages to queues directly

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        queue (str): The name of the queue that receives the messages to be published
        message (str): The published message
        durable (boolean): If True, the broker will not remove the queue in case of a restart

        Usage examples
        --------------
        >>> sent_to(ch, 'logs', 'A critical kernel error')
        """
        # NOTE: By default, it operates using Round-Robin dispatching.
        if queue is None or queue == '':
            raise Exception('In direct queues, queues must be named!')
        if durable:
            kwargs.update({'properties': pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)})
        channel.queue_declare(queue=queue, durable=durable)
        channel.basic_publish(exchange='', routing_key=queue, body=message, **kwargs)

    @staticmethod
    def broadcast_to(channel, exchange: str, message: str):
        """Broadcast messages to multiple consumers

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that publishes the messages
        message (str): The published message

        Usage examples
        --------------
        >>> .broadcast_to(ch, 'logs', 'A critical kernel error')
        """
        if exchange is None or exchange == '':
            raise Exception('In fanout, exchange must be named!')
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        channel.basic_publish(exchange=exchange, routing_key='', body=message)

    @staticmethod
    def direct_to(channel, exchange: str, routing: str, message: str):
        """Delivers messages to multiple queues

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that publishes the messages
        routing (str): The name of the queue that listen for the expected messages
        message (str): The published message

        Usage examples
        --------------
        >>> .direct_to(ch, 'logs', 'critical', 'A critical kernel error')
        """
        if exchange is None or exchange == '':
            raise Exception('In direct, exchange must be named!')
        if routing is None or routing == '':
            raise Exception('In direct, routing must be named!')
        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        channel.basic_publish(exchange=exchange, routing_key=routing, body=message)

    @staticmethod
    def topic_to(channel, exchange: str, routing: str, message: str):
        """Delivers messages to multiple queues based on pattern

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        routing (str): A string of words, delimited by dots
        message (str): The published message

        Usage examples
        --------------
        >>> .topic_to(ch, 'logs', 'kernel.critical.print', 'A critical kernel error')
        >>> .topic_to(ch, 'logs', 'kernel.critical.file', 'A critical kernel error')
        >>> .topic_to(ch, 'logs', 'kernel.#', 'A critical kernel error')
        >>> .topic_to(ch, 'logs', '*.critical.*', 'A critical kernel error')
        """
        if exchange is None or exchange == '':
            raise Exception('In topic, exchange must be named!')
        if routing is None or routing == '':
            raise Exception('In topic, routing must be named!')
        channel.exchange_declare(exchange=exchange, exchange_type='topic')
        channel.basic_publish(exchange=exchange, routing_key=routing, body=message)

    #
    # Receive Messages
    #
    @staticmethod
    def receive_from(channel, queue: str, callback, ack: bool = True, durable: bool = False, **kwargs):
        """Receives messages from queues directly
            - By default, it operates using Round-Robin dispatching.
            - if 'ack=True' then add 'ch.basic_ack(delivery_tag=method.delivery_tag)' and the end of the

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        queue (str): The name of the queue that delivers the expected messages to be consumed
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]
        durable (boolean): If True, the broker will not remove the queue in case of a restart

        Usage examples
        --------------
        >>> .receive_from(ch, 'logs', fun)
        """
        if queue is None or queue == '':
            raise Exception('In direct exchange, queues must be named!')
        channel.queue_declare(queue=queue, durable=durable)
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=ack, **kwargs)

    @staticmethod
    def from_broadcast(channel, exchange: str, callback, ack: bool = True, **kwargs):
        """Receives messages from broadcasted exchanges
            - By default the queue is exclusive to the exchange so the name is a random one

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]

        Usage examples
        --------------
        >>> .from_broadcast(ch, 'logs', fun)
        """
        if exchange is None or exchange == '':
            raise Exception('In fanout, exchange must be named!')
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        queue = channel.queue_declare(queue='', exclusive=True)
        channel.queue_bind(exchange=exchange, queue=queue.method.queue)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)

    @staticmethod
    def from_direct(channel, exchange: str, routing: Union[str, List[str]], callback, ack: bool = True, **kwargs):
        """Receives messages from multiple routes

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        routing (list(str)): A list of all the routes to listen to
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]

        Usage examples
        --------------
        >>> .from_direct(ch, 'logs', 'critical', fun)
        """
        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        queue = channel.queue_declare(queue='', exclusive=True)
        routing = routing if isinstance(routing, list) else [routing]
        for r in routing:
            channel.queue_bind(exchange=exchange, queue=queue.method.queue, routing_key=r)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)

    @staticmethod
    def from_topic(channel, exchange: str, routing: Union[str, List[str]], callback, ack: bool = True, **kwargs):
        """Receives messages from multiple topics

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        routing (list(str)): A list of all the routes to listen to
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]

        Usage examples
        --------------
        >>> .from_topic(ch, 'logs', 'kernel.critical.print', fun)
        >>> .from_topic(ch, 'logs', 'kernel.#', fun)
        >>> .from_topic(ch, 'logs', '*.critical.*', fun)
        """
        channel.exchange_declare(exchange=exchange, exchange_type='topic')
        queue = channel.queue_declare('', exclusive=True)
        routing = routing if isinstance(routing, list) else [routing]
        for r in routing:
            channel.queue_bind(exchange=exchange, queue=queue.method.queue, routing_key=r)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OneRabbitMQClient(RabbitMQClient, metaclass=Singleton):
    pass

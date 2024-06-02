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
import ssl
import time
import logging
from threading import Thread
from typing import Union, List
from functools import wraps, partial
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
# Decorators
# ==================================================================================================
#
def threaded(f):
    """This function runs a function in a separate thread"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        thread = Thread(target=f, args=args, kwargs=kwargs)
        thread.start()
    return wrapper


# ==================================================================================================
# Functions
# ==================================================================================================
@threaded
def callback_threaded(func, ch, method, properties, body, connection):
    """This function runs a callback in a separate thread to solve the long running task problem,
       where the heartbeat is not send because the main thread is blocked with the callback.
       It runs the function and sends the ACK manually using the thread safe function
    """
    logger.info(f"{func.__name__} running in a separate thread")
    func(ch, method, properties, body)
    cb = partial(RabbitMQClient.ack_message, ch, method.delivery_tag)
    connection.add_callback_threadsafe(cb)


def is_callback_threaded(callback, threaded, ack, **kwargs):
    """This function converts a callback function to threaded one"""
    # Extract the the connection either way, because in case the Subscriber class is given, then it
    # is always given. Remove it so does not send as argument to pika's library functions
    connection = kwargs.pop('connection', None)
    # If threaded argument is True the set ACK to False so it sent manually
    if threaded:
        # Always change ACK to False in threaded callback
        ack = False
        if connection is None:
            raise Exception('Callback cannot run in a separate thread because the connection not given!')
        callback = partial(callback_threaded, callback, connection=connection)
    return callback, ack, kwargs


# ==================================================================================================
# Classes
# ==================================================================================================
#
class RabbitMQClient:

    def __init__(self, host, port, username=None, password=None, verbose: bool = True):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl_options = None
        # If port is SSL one then connect with SSL enabled
        if self.port == 5671:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            self.ssl_options = pika.SSLOptions(context)
        self.connection = None
        self.channel = None
        self.connected = False
        if not verbose:
            self.verbose = verbose
            logging.getLogger('pika').setLevel(logging.WARNING)

    # -------------------------------------
    # Connections
    # -------------------------------------
    def connect(self):
        """Connects to the RabbitMQ server, retrying on failure"""
        max_retries, attempts = 3, 0
        while not self.connected and attempts < max_retries:
            try:
                credentials=PlainCredentials(self.username, self.password)
                connection_parameters = pika.ConnectionParameters(host=self.host,
                                                                  port=self.port,
                                                                  credentials=credentials,
                                                                  ssl_options=self.ssl_options)
                self.connection = pika.BlockingConnection(connection_parameters)
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
            if self.channel.is_open:
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
            self.channel.start_consuming()

    def stop(self):
        """Stops consuming messages from subscribed queues"""
        if self.is_connected():
            self.channel.stop_consuming()

    def heartbeat(self):
        """Sends a forced heartbeat to the broker"""
        # NOTE: Use this function in case of a long running publisher
        self.connection.process_data_events(time_limit=1)

    def terminate(self):
        """Steps and disconnects this client"""
        self.stop()
        self.disconnect()

    # -------------------------------------
    # Messaging
    # -------------------------------------
    @staticmethod
    def ack_message(channel, delivery_tag):
        """Send manually an ACK to the broker
        NOTE: Channel must be the same pika channel instance via which the message being ACKed was
              retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

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
    def receive_from(channel, queue: str, callback, ack: bool = True, durable: bool = False,
                     threaded: bool = False, **kwargs):
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
        threaded (boolean): If True, runs the callback in a separate threat. It is recommends for
                            long running tasks [Default: false]

        Usage examples
        --------------
        >>> .receive_from(ch, 'logs', fun)
        """
        # Check if callback should run in a separate threat
        callback, ack, kwargs = is_callback_threaded(callback, threaded, ack, **kwargs)

        if queue is None or queue == '':
            raise Exception('In direct exchange, queues must be named!')
        channel.queue_declare(queue=queue, durable=durable)
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=ack, **kwargs)
        logger.info(f"Subscribe directly to queue '{queue}'")

    @staticmethod
    def from_broadcast(channel, exchange: str, callback, ack: bool = True, threaded: bool = False, **kwargs):
        """Receives messages from broadcasted exchanges
            - By default the queue is exclusive to the exchange so the name is a random one
            - if 'ack=True' then add 'ch.basic_ack(delivery_tag=method.delivery_tag)' and the end of the

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]
        threaded (boolean): If True, runs the callback in a separate threat. It is recommends for
                            long running tasks [Default: false]

        Usage examples
        --------------
        >>> .from_broadcast(ch, 'logs', fun)
        """
        # Check if callback should run in a separate threat
        callback, ack, kwargs = is_callback_threaded(callback, threaded, ack, **kwargs)

        if exchange is None or exchange == '':
            raise Exception('In fanout, exchange must be named!')
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        queue = channel.queue_declare(queue='', exclusive=True)
        channel.queue_bind(exchange=exchange, queue=queue.method.queue)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)
        logger.info(f"Subscribe to broadcast exchange '{exchange}'")

    @staticmethod
    def from_direct(channel, exchange: str, routing: Union[str, List[str]], callback, ack: bool = True,
                    threaded: bool = False, **kwargs):
        """Receives messages from multiple routes
            - if 'ack=True' then add 'ch.basic_ack(delivery_tag=method.delivery_tag)' and the end of the

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        routing (list(str)): A list of all the routes to listen to
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]
        threaded (boolean): If True, runs the callback in a separate threat. It is recommends for
                            long running tasks [Default: false]

        Usage examples
        --------------
        >>> .from_direct(ch, 'logs', 'critical', fun)
        """
        # Check if callback should run in a separate threat
        callback, ack, kwargs = is_callback_threaded(callback, threaded, ack, **kwargs)

        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        queue = channel.queue_declare(queue='', exclusive=True)
        routing = routing if isinstance(routing, list) else [routing]
        for r in routing:
            channel.queue_bind(exchange=exchange, queue=queue.method.queue, routing_key=r)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)
        logger.info(f"Subscribe to direct exchange '{exchange}' via routing '{routing}'")

    @staticmethod
    def from_topic(channel, exchange: str, routing: Union[str, List[str]], callback, ack: bool = True,
                   threaded: bool = False, **kwargs):
        """Receives messages from multiple topics
            - if 'ack=True' then add 'ch.basic_ack(delivery_tag=method.delivery_tag)' and the end of the

        Parameters
        ----------
        channel (rabbitMQ): A rabbitMQ communication channel between client and broker
        exchange (str): The name of the exchange that broadcasts the messages to be consumed
        routing (list(str)): A list of all the routes to listen to
        callback (function): An iteration function of the model
        ack (boolean): If True, disables manual message acknowledgments [Default: True]
        threaded (boolean): If True, runs the callback in a separate threat. It is recommends for
                            long running tasks [Default: false]

        Usage examples
        --------------
        >>> .from_topic(ch, 'logs', 'kernel.critical.print', fun)
        >>> .from_topic(ch, 'logs', 'kernel.#', fun)
        >>> .from_topic(ch, 'logs', '*.critical.*', fun)
        """
        # Check if callback should run in a separate threat
        callback, ack, kwargs = is_callback_threaded(callback, threaded, ack, **kwargs)

        channel.exchange_declare(exchange=exchange, exchange_type='topic')
        queue = channel.queue_declare('', exclusive=True)
        routing = routing if isinstance(routing, list) else [routing]
        for r in routing:
            channel.queue_bind(exchange=exchange, queue=queue.method.queue, routing_key=r)
        channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=ack, **kwargs)
        logger.info(f"Subscribe to topic '{exchange}' via routing '{routing}'")


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OneRabbitMQClient(RabbitMQClient, metaclass=Singleton):
    pass

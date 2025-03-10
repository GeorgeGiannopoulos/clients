# utilities.py -------------------------------------------------------------------------------------
#
# Description:
#    This script contains general funtions
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import os
import sys
import logging
from contextlib import contextmanager
from typing import Union, List, Callable
# Installed
from pydantic import BaseModel
# Custom
from rabbitmq_client.client import RabbitMQClient


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Manager
# ==================================================================================================
#
@contextmanager
def subscriber(client: RabbitMQClient, exit_on_failure: bool = False):
    """A wrapper manager for subscription"""
    try:
        logger.info('Starting rabbitMQ client...')
        logger.info('Connect rabbitMQ client')
        client.connect()
        yield client.channel
        logger.info('Start Consuming...')
        client.run()
    except KeyboardInterrupt:
        yield None  # NOTE: yield should run at least once even in exception
        raise KeyboardInterrupt() from None
    except Exception as e:
        logger.error(e)
        yield None  # NOTE: yield should run at least once even in exception
    finally:
        if client.is_connected():
            logger.info("Prepare to terminate rabbitMQ client gracefully...")
            logger.info('Stop Consuming...')
            client.stop()
            logger.info('Disconnect rabbitMQ client')
            client.disconnect()
        # Shutdown eventually
        if exit_on_failure:
            logger.info("Exiting...")
            try:
                sys.exit(1)
            except:
                os._exit(1)


@contextmanager
def publisher(client: RabbitMQClient):
    """A wrapper manager for publication"""
    try:
        logger.info('Starting rabbitMQ client...')
        logger.info('Connect rabbitMQ client')
        client.connect()
        yield client.channel
    except KeyboardInterrupt:
        yield None  # NOTE: yield should run at least once even in exception
        raise KeyboardInterrupt() from None
    except Exception as e:
        logger.error(e)
        yield None  # NOTE: yield should run at least once even in exception
    finally:
        if client.is_connected():
            logger.info("Prepare to terminate rabbitMQ client gracefully...")
            logger.info('Disconnect rabbitMQ client')
            client.disconnect()


# ==================================================================================================
# Models
# ==================================================================================================
#
class DirectlySubscription(BaseModel):
    queue: str
    callback: Callable
    kwargs: dict


class BroadcastSubscription(BaseModel):
    exchange: str
    callback: Callable
    kwargs: dict


class DirectSubscription(BaseModel):
    exchange: str
    routing: Union[str, List[str]]
    callback: Callable
    kwargs: dict


class TopicSubscription(BaseModel):
    exchange: str
    routing: Union[str, List[str]]
    callback: Callable
    kwargs: dict


# ==================================================================================================
# Class
# ==================================================================================================
#
class Subscription:
    """A class for managing subscriptions to various message queues and exchanges."""

    def __init__(self):
        self._directly = list()
        self._broadcasts = list()
        self._directs = list()
        self._topics = list()

    def directly(self, queue: str, **kwargs):
        """Decorator for subscribing to messages from a specific queue.

        Parameters
        ----------
        queue (str): The name of the queue to subscribe to.
        **kwargs: Additional keyword arguments.

        Usage examples
        --------------
        >>> @sub.directly('logs')
            def fun(ch, method, properties, body):
        """
        def decorator(func):
            self._directly.append(DirectlySubscription(queue=queue, callback=func, kwargs=kwargs))
            return func
        return decorator

    def broadcast(self, exchange: str, **kwargs):
        """Decorator for subscribing to broadcast messages from an exchange.

        Parameters
        ----------
        exchange (str): The name of the exchange to subscribe to.
        **kwargs: Additional keyword arguments.

        Usage examples
        --------------
        >>> @sub.broadcast('logs')
            def fun(ch, method, properties, body):
        """
        def decorator(func):
            self._broadcasts.append(BroadcastSubscription(exchange=exchange, callback=func, kwargs=kwargs))
            return func
        return decorator

    def direct(self, exchange: str, routing: Union[str, List[str]], **kwargs):
        """
        Decorator for subscribing to direct messages from an exchange with specific routing key(s).

        Parameters
        ----------
        exchange (str): The name of the exchange to subscribe to.
        routing (str | List[str]): The routing key(s) for the subscription.
        **kwargs: Additional keyword arguments.

        Usage examples
        --------------
        >>> @sub.topic('logs', 'critical')
            def fun(ch, method, properties, body):
        """
        def decorator(func):
            self._directs.append(DirectSubscription(exchange=exchange, routing=routing, callback=func, kwargs=kwargs))
            return func
        return decorator

    def topic(self, exchange: str, routing: Union[str, List[str]], **kwargs):
        """Decorator for subscribing to messages from a topic exchange with specific routing pattern(s).

        Parameters
        ----------
        exchange (str): The name of the exchange to subscribe to.
        routing (str | List[str]): The routing pattern(s) for the subscription.
        **kwargs: Additional keyword arguments.

        Usage examples
        --------------
        >>> @sub.topic('logs', 'kernel.critical.*')
            def fun(ch, method, properties, body):
        """
        def decorator(func):
            self._topics.append(TopicSubscription(exchange=exchange, routing=routing, callback=func, kwargs=kwargs))
            return func
        return decorator


class Subscriber:
    """A class for managing subscribers to RabbitMQ queues and exchanges.

    Parameters
    ----------
    client (RabbitMQClient): The RabbitMQ client instance.
    subscriptions (Subscription | List[Subscription]): The subscription(s) to handle.
    """

    def __init__(self, client: RabbitMQClient, subscriptions: Union[Subscription, List[Subscription]]):
        self._client = client
        self._subscriptions = subscriptions if isinstance(subscriptions, List) else [subscriptions]
        self._directly = list()
        self._broadcasts = list()
        self._directs = list()
        self._topics = list()
        for _subscription in self._subscriptions:
            self._directly.extend(_subscription._directly)
            self._broadcasts.extend(_subscription._broadcasts)
            self._directs.extend(_subscription._directs)
            self._topics.extend(_subscription._topics)

    def run(self, exit_on_failure: bool=False):
        """Start processing subscriptions by running the RabbitMQ client."""
        with subscriber(self._client, exit_on_failure) as channel:
            if channel:
                for directly in self._directly:
                    self._client.receive_from(channel, directly.queue, directly.callback,
                                              connection=self._client.connection, **directly.kwargs)
                for broadcast in self._broadcasts:
                    self._client.from_broadcast(channel, broadcast.exchange, broadcast.callback,
                                                connection=self._client.connection, **broadcast.kwargs)
                for direct in self._directs:
                    self._client.from_direct(channel, direct.exchange, direct.routing, direct.callback,
                                             connection=self._client.connection, **direct.kwargs)
                for topic in self._topics:
                    self._client.from_topic(channel, topic.exchange, topic.routing, topic.callback,
                                            connection=self._client.connection, **topic.kwargs)


# ==================================================================================================
# Functions
# ==================================================================================================
def callback_logger(ch, method, properties, body, tracking_id: str = None, message: str=None, level=logging.INFO):
    try:
        # Tracking ID
        tracking_id = f"{tracking_id} " if tracking_id else ''
        # Μessage
        message = f" {message}" if message else ''
        # Identity info
        consumer_tag = getattr(method, 'consumer_tag', '-')
        delivery_tag = getattr(method, 'delivery_tag', '-')
        # Delivery path info
        exchange = getattr(method, 'exchange', '-')
        routing_key = getattr(method, 'routing_key', '-')
        queue = getattr(method, 'queue', '-')
        destination = f"{exchange} > {routing_key} > {queue}"
        # Delivery properties info
        delivery_mode = getattr(properties, 'delivery_mode', '-')

        msg = f"{tracking_id}{consumer_tag} {delivery_tag} '{destination}'{message}"
    except:
        level = logging.ERROR
        msg = 'Failed to log for this delivery!'
    logger.log(level, msg)

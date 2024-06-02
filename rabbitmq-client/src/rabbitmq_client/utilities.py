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
def subscriber(client: RabbitMQClient):
    """A wrapper manager for subscription"""
    try:
        logger.info('Starting rabbitMQ client...')
        logger.info('Connect rabbitMQ client')
        client.connect()
        yield client.channel
        logger.info('Start Consuming...')
        client.run()
    except KeyboardInterrupt:
        logger.info("Prepare to terminate rabbitMQ client gracefully...")
        logger.info('Stop Consuming...')
        client.stop()
        logger.info('Disconnect rabbitMQ client')
        client.disconnect()
        # Shutdown eventually
        logger.info("Exiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as e:
        logger.exception(e)



@contextmanager
def publiser(client: RabbitMQClient):
    """A wrapper manager for publication"""
    try:
        logger.info('Starting rabbitMQ client...')
        logger.info('Connect rabbitMQ client')
        client.connect()
        yield client.channel
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
    finally:
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

    def run(self):
        """Start processing subscriptions by running the RabbitMQ client."""
        with subscriber(self._client) as channel:
            for directly in self._directly:
                self._client.receive_from(channel, directly.queue, directly.callback, **directly.kwargs)
            for broadcast in self._broadcasts:
                self._client.from_broadcast(channel, broadcast.exchange, broadcast.callback, **broadcast.kwargs)
            for direct in self._directs:
                self._client.from_direct(channel, direct.exchange, direct.routing, direct.callback, **direct.kwargs)
            for topic in self._topics:
                self._client.from_topic(channel, topic.exchange, topic.routing, topic.callback, **topic.kwargs)

# RabbitMQ Client wrapper

A client that connects to [RabbitMQ Broker](https://github.com/GeorgeGiannopoulos/dockerfiles/tree/master/rabbitmq_broker)

## How to install

For Production:

```shell
pip install -r requirements.txt
pip install .
```

For Deployment:

```shell
pip install -r requirements.txt
pip install -e .
```

## How to use

Simple use of the client

```python
from rabbitmq_client.client import RabbitMQClient
from rabbitmq_client.utilities import subscriber

client = RabbitMQClient(host='localhost')

def callback(ch, method, properties, body):
    # Implement your callback logic here
    # ...
    sent_to(ch, 'outgoing-queue', 'a-message')

with subscriber(client) as channel:
    client.receive_from(channel, 'incoming-queue', callback)
```

Advanced use of the client

```python
from rabbitmq_client.client import RabbitMQClient
from rabbitmq_client.utilities import Subscription, Subscriber

client = RabbitMQClient(host='localhost')
subscriptions = Subscription()

@subscriptions.directly('incoming-queue')
def callback(ch, method, properties, body):
    # Implement your callback logic here
    # ...
    client.sent_to(ch, 'outgoing-queue', 'a-message')

Subscriber(client, subscriptions).run()
```

## How to test

**TODO**

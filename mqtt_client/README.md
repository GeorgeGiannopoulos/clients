# MQTT Client wrapper

A client that connects to [MQTT Broker](https://github.com/GeorgeGiannopoulos/dockerfiles/tree/master/mqtt_broker)

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

```python
from mqtt_client.clients import MQTTClient
from src.mqtt_client.utilities import subscriber

client = MQTTClient(host=MQTT_HOST, username=MQTT_USER, password=MQTT_PASS)

def callback(payload):
    # Implement your callback logic here
    # ...
    client.publish('/topic/to/publish', 'test')

with subscriber(client) as client:
    client.subscribe('/topic/to/subscribe', callback)

    while True:
        time.sleep(1)
```

## How to test

**TODO**

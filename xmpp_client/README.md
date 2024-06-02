# XMPP Client wrapper

A client that connects to [XMPP Server](https://github.com/GeorgeGiannopoulos/dockerfiles/tree/master/xmpp_server)

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
from xmpp_client.client import XMPPClient
from xmpp_client.utilities import subscriber, publisher

client = XMPPClient('localhost', username='user', password='4s3r!', nick='user', resource='localhost')

def callback(payload):
    # Implement your callback logic here
    # ...
    client.send('test', 'test@conference.localhost')

with subscriber(client) as client:
    # Send test message to room
    client.join_room('test@conference.localhost', callback)
```

## How to test

**TODO**

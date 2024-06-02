# client.py ----------------------------------------------------------------------------------------
#
# Description:
#    This script contains the MQTT class
#
# NOTE:
# - This Class uses .loop_start() function that runs in a network thread. In order to run, the
#   main process must stay alive. In the current version the main process stays alive due to
#   .run() function of flask
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import ssl
import time
import uuid
import logging
# Installed
import paho.mqtt.client as mqtt
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
class MQTTClient:
    """A MQTT client that will connect to a Broker, receive and send messages"""

    def __init__(self, host, port=1883, username=None, password=None, tls=None, client_id='mqtt-client'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.tls = tls
        self.client_id = f"{client_id}:{uuid.uuid4().hex}"
        self.connected = False
        self._subscription = dict()

        self.client = mqtt.Client(self.client_id)
        # Assign event callbacks
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_unsubscribe = self.on_unsubscribe
        self.client.on_log = self.on_log
        # Set connection type
        self._credentials()

    # -------------------------------------
    # MQTT callbacks
    # -------------------------------------
    def on_connect(self, mqttc, obj, flags, rc):
        """This function is the on_connect callback function, used by MQTT client"""
        print("{} {} {} {}".format(mqttc, obj, flags, rc))
        if rc == 0:
            logger.info("Connected: {}".format(True))
        else:
            logger.critical("Connected: {}".format(False))

    def on_disconnect(self, mqttc, obj, rc):
        """This function is the on_disconnect callback function, used by MQTT client"""
        print("{} {} {} {}".format(mqttc, obj, rc))
        if rc == 0:
            logger.info("Disconnected: {}".format(True))
        else:
            logger.critical("Disconnected: {}".format(False))

    def on_message(self, mqttc, obj, msg):
        """This function is the on_message callback function, used by MQTT client"""
        # Decode Payload
        payload = msg.payload
        msg_decode = payload.decode("utf-8")
        try:
            self._subscription[msg.topic](msg_decode)
        except KeyError as e:
            logger.exception(e)
            logger.warning(f"No handler function defined for topic '{msg.topic}'")

    def on_publish(self, mqttc, obj, mid):
        """This function is the on_publish callback function, used by MQTT client"""
        logger.debug("Query Counter (publish): {}".format(mid))

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        """This function is the on_subscribe callback function, used by MQTT client"""
        logger.debug("Query Counter (subscribe): {}, QoS: {}".format(mid, granted_qos))

    def on_unsubscribe(self, mqttc, obj, mid):
        """This function is the on_unsubscribe callback function, used by MQTT client"""
        logger.debug("Query Counter (unsubscribe): {}".format(mid))

    def on_log(self, mqttc, obj, level, string):
        """This function is the on_log callback function, used by MQTT client"""
        logger.debug("{}".format(string))

    # -------------------------------------
    # Private methods
    # -------------------------------------
    def _credentials(self):
        """This function set the correct credentials to the client"""
        if self.username != None and self.password != None:
            self.client.username_pw_set(self.username, self.password)
        elif self.tls != None:
            # Fallback to 'ssl.PROTOCOL_SSLv23' in order to support Python < 3.5.3.
            SSL_PROTOCOL_VERSION = getattr(ssl, "PROTOCOL_TLS", ssl.PROTOCOL_SSLv23)
            self.client.tls_set(ca_certs=None,
                                certfile=None,
                                keyfile=None,
                                cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=SSL_PROTOCOL_VERSION,
                                ciphers=None
                                )

    def _ack(self):
        """There seems to be a problem receiving the ACK messages, so to fix it start, and then stop the loop"""
        # Start-Stop loop to receive the connection's ACK messages
        self.client.loop_start()
        time.sleep(0.5)
        self.client.loop_stop()

    # -------------------------------------
    # Connections
    # -------------------------------------
    def connect(self):
        """This function connects the client to the broker"""
        tries = 3
        for i in range(1, tries + 1):
            try:
                # Try to connect to MQTT Client
                self.client.connect(self.host, self.port)
                self._ack()
                self.connected = True
                break
            except:
                self.connected = False
                logger.error("[{}/{}] Failed to connect to MQTT Broken".format(i, tries))
                time.sleep(1)
        if not self.connected:
            raise Exception("Failed to connect to MQTT Broken")

    def disconnect(self):
        """This function disconnects the client from the broker"""
        # Disconnect
        self.client.disconnect()
        self._ack()
        self.connected = False

    def is_connected(self):
        """Checks if connected to the server"""
        return self.connected

    def run(self):
        """This function starts the listener"""
        if self.connected:
            self.client.loop_start()

    def stop(self):
        """This function stops the listener"""
        if self.connected:
            self.client.loop_stop()

    # -------------------------------------
    # Messaging
    # -------------------------------------
    def unsubscribe(self, topic: str):
        """This function unsubscribes from a topic"""
        self.client.unsubscribe(topic)
        del self._subscription[topic]

    def subscribe(self, topic: str, callback, qos: int = 1):
        """This function subscribes to a topic"""
        self.client.subscribe(topic, qos=qos)
        self._subscription[topic] = callback

    def publish(self, topic, payload, qos: int = 1):
        """This function publishes a payload to a topic"""
        self.client.publish(topic, payload=payload, qos=qos)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OneRabbitMQClient(MQTTClient, metaclass=Singleton):
    pass

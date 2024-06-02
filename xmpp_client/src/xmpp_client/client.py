# client.py ----------------------------------------------------------------------------------------
#
# Description:
#    This script contains the MCU XMPP class
#
# NOTE:
# - Prior to running this suite, a user and a room mentioned in the config file must be created
#   in the openfire server
# - This Class uses .process() function that runs in a network thread. In order to run, the
#   main process must stay alive. In the current version the main process stays alive due to
#   .run() function of flask
# - SleekXMPP is deprecated in favor of Slixmpp, a fork which takes full advantage of Python 3 and
#   asyncio. However SleekXMPP is being used because it runs on a separate thread using the flag
#   block=False (default) as an argument on .process() functions. The new one don't run on a thread,
#   so additional code should implemented to run on a thread
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import time
import logging
# Installed
import sleekxmpp
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
class XMPPClient:
    """An XMPP client that will connect to a room, receive and send messages"""

    def __init__(self, host, port=5222, username=None, password=None, nick=None, resource='openfire'):
        self.host = host
        self.port = port
        self.password = password
        self.nick = nick
        self.jid = '{}@{}/{}'.format(username, host, resource)
        self.connected = False
        self.rooms = dict()

    # -------------------------------------
    # XMPP callbacks
    # -------------------------------------
    def registration(self):
        self.client = sleekxmpp.ClientXMPP(self.jid, self.password)
        # NOTE: If you are working with an OpenFire server, you will
        #       need to use a different SSL version:
        # import ssl
        # self.client.ssl_version = ssl.PROTOCOL_SSLv3

        # Register Plugins
        self.client.register_plugin('xep_0030')  # Service Discovery
        self.client.register_plugin('xep_0045')  # Multi-User Chat
        self.client.register_plugin('xep_0060')  # v  pubsub
        self.client.register_plugin('xep_0199')  # XMPP Ping
        self.client.register_plugin('xep_0198')  # XMPP streaming messages
        self.client.register_plugin('xep_0184')  # XMPP streaming messages

        # Register events
        self.client.add_event_handler("session_start", self.start)
        self.client.add_event_handler("groupchat_message", self.muc_message)
        self.client.add_event_handler("groupchat_presence", self.muc_presence)
        for room in self.rooms.keys():
            self.client.add_event_handler("muc::%s::got_online" % room, self.muc_online)
            self.client.add_event_handler("muc::%s::got_offline" % room, self.muc_offline)

        # HACK: The following lines were added to skip invalid SSL cert
        #       If the modules "pyasn1" and "pyasn1-modules" are installed then
        #       either remove them or commented-in the following lines
        #       Tested with:
        #       - pyasn1==0.3.6
        #       - pyasn1-modules==0.1.5
        # def _discard(self, event):
        #     return
        # self.add_event_handler("ssl_invalid_cert", self._discard)

    def start(self, event):
        """This function handles the starting session event"""
        self.client.get_roster()
        self.client.send_presence()
        # Join rooms
        self._join_rooms()

    def muc_presence(self, presence):
        """This function is triggered when a presence arrives at MUC"""
        if presence['muc']['nick'] == self.nick:
            self._configure_room(presence['from'].bare)

    def muc_online(self, presence):
        """ This function is triggered when a new user enter the room, even this client"""
        # Check if the message was sent by this client (ignore it in this case)
        if presence['muc']['nick'] != self.nick:
            logger.info("User '{}' entered the room '{}'".format(presence['muc']['nick'], presence['from'].bare))
        else:
            logger.info("I entered the room '{}'".format(presence['from'].bare))

    def muc_offline(self, presence):
        """This function is triggered when a user leaves a room, even this client"""
        # Check if the message was sent by this client (ignore it in this case)
        if presence['muc']['nick'] != self.nick:
            logger.info("User '{}' left the room '{}'".format(presence['muc']['nick'], presence['from'].bare))
        else:
            logger.info("I left the room '{}'".format(presence['from'].bare))

    def muc_message(self, msg):
        """This function handles the incoming messages"""
        # NOTE [IMPORTANT]: Always check that a message is not from yourself, otherwise you will
        #                   create an infinite loop responding to your own messages
        # Check if the message was sent by this client (ignore it in this case)
        if msg['mucnick'] != self.nick:
            logger.info("User '{}' sent a message to the room '{}'".format(msg['mucnick'],
                                                                           msg['from'].bare))
            try:
                self.rooms[msg['from'].bare](msg['body'])
            except Exception as e:
                logger.error("Failed to read message!")
                logger.exception(e)
        else:
            logger.info("I sent a message to the room '{}'".format(msg['from'].bare))

    # -------------------------------------
    # Rooms
    # -------------------------------------
    def _configure_room(self, room):
        """This function configures a room, after joining it"""
        # NOTE: Room configuration can only be done once a MUC presence stanza has been received
        #       from the server.
        try:
            muc = self.client.plugin['xep_0045']
            muc.getRoomConfig(room)
            form = muc.getRoomConfig(room)
            muc.configureRoom(room, form)
        except:
            pass

    def _join_rooms(self):
        """This function joins the client to all needed rooms"""
        muc = self.client.plugin['xep_0045']
        for room in self.rooms.keys():
            muc.joinMUC(room,
                        self.nick,
                        # If a room password is needed, use:
                        # password=the_room_password,
                        wait=True)

    def _leave_rooms(self):
        """This function exits the client from all needed rooms"""
        muc = self.client.plugin['xep_0045']
        for room in self.rooms.keys():
            logger.info("Leaving room '{}'".format(room))
            if room in muc.rooms.keys():
                muc.leaveMUC(room, self.nick)
            else:
                logger.error("Not in room '{}'".format(room))

    # -------------------------------------
    # Connections
    # -------------------------------------
    def connect(self):
        """This function connects the client to the server"""
        try:
            # Connect to xmpp server
            if self.client.connect():
                self.connected = True
            else:
                self.connected = False
                # Disconnect
                self.client.disconnect()
                message = "Failed to connect to XMPP Server (Openfire)"
                logger.critical(message)
                raise Exception(message)
        except:
            self.connected = False
            # Disconnect
            self.client.disconnect()

    def disconnect(self):
        """This function disconnects the client from the server"""
        # Leave from all joined rooms
        self._leave_rooms()
        # Disconnect
        self.client.disconnect()
        self.connected = False

    def is_connected(self):
        """Checks if connected to the server"""
        return self.connected

    def run(self):
        """This function starts the listener"""
        if self.connected:
            self.client.process(block=False)
            # Wait for the .process() to finish
            time.sleep(2)

    # -------------------------------------
    # Messaging
    # -------------------------------------
    def join_room(self, room: str, callback):
        """This function joins the client to a rooms"""
        self.rooms[room] = callback

    def leave_room(self, room: str):
        """This function exits the client from a rooms"""
        del self.rooms[room]

    def send(self, message, room):
        """This function Sends a message to a room"""
        print(message)
        self.client.send_message(mto=room,
                                 mbody=message,
                                 mtype='groupchat')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OneXMPPClient(XMPPClient, metaclass=Singleton):
    pass

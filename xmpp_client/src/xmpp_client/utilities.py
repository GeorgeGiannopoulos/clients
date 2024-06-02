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
# Installed
# NOTE: Add here all the Installed modules
# Custom
from xmpp_client.client import XMPPClient


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Manager
# ==================================================================================================
#
@contextmanager
def subscriber(client: XMPPClient):
    """A wrapper manager for subscription"""
    try:
        logger.info('Starting XMPP client...')
        logger.info('Connect XMPP client')
        yield client
        client.registration()
        client.connect()
        logger.info('Start Consuming...')
        client.run()
    except KeyboardInterrupt:
        logger.info("Prepare to terminate XMPP client gracefully...")
        logger.info('Disconnect XMPP client')
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
def publisher(client: XMPPClient):
    """A wrapper manager for publication"""
    try:
        logger.info('Starting XMPP client...')
        logger.info('Connect XMPP client')
        client.connect()
        yield client
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("Prepare to terminate XMPP client gracefully...")
        logger.info('Disconnect XMPP client')
        client.disconnect()

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
from mqtt_client.client import MQTTClient


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Manager
# ==================================================================================================
#
@contextmanager
def subscriber(client: MQTTClient):
    """A wrapper manager for subscription"""
    try:
        logger.info('Starting MQTTClient client...')
        logger.info('Connect MQTTClient client')
        client.connect()
        yield client
        logger.info('Start Consuming...')
        client.run()
    except KeyboardInterrupt:
        logger.info("Prepare to terminate MQTTClient client gracefully...")
        logger.info('Stop Consuming...')
        client.stop()
        logger.info('Disconnect MQTTClient client')
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
def publisher(client: MQTTClient):
    """A wrapper manager for publication"""
    try:
        logger.info('Starting MQTTClient client...')
        logger.info('Connect MQTTClient client')
        client.connect()
        yield client
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("Prepare to terminate MQTTClient client gracefully...")
        logger.info('Disconnect MQTTClient client')
        client.disconnect()

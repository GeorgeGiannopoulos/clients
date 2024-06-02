# utilities.py -------------------------------------------------------------------------------------
#
# Description:
#    This script contains utilities
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import logging
# Installed
import pandas as pd  # NOTE: This is installed as part of [extra] argument on requirements.txt
# Custom
from influx_client import InfluxClient


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Functions
# ==================================================================================================
#
def df_from_db(client: InfluxClient, bucket: str, measurement: str, fields: str | list = None, method=None, **kwargs):
    """Fetch data in DataFrame format"""
    # Fetch data
    df = client.read(bucket, measurement, fields, method, format='dataframe', **kwargs)
    # Check Data and convert them to Dataframe list
    if not isinstance(df, list):
        if not isinstance(df, pd.DataFrame):
            return None
        df = [df]
    df = pd.concat(df)

    if df.empty:
        return None
    df = df.drop(columns=['result', 'table', '_start', '_stop', '_measurement'], errors='ignore')
    df = df.rename(columns={'_time': 'timestamp'})
    return df

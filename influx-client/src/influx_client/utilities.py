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
import ast
import json
import logging
import datetime
# Installed
import pandas as pd  # NOTE: This is installed as part of [extra] argument on requirements.txt
# Custom
# NOTE: Add here all the Custom modules


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Functions
# ==================================================================================================
#
def to_dict(df):
    """This function coverts InfluxDB DataFrame output to dictionary"""
    return json.loads(df.to_json(orient='records'))


def pivot_df(df, on_empty: str = None):
    """This function checks if InfluxDB DataFrame output is empty or a list and concatenates it"""
    # If not list then,
    #     If not a DataFrame, then return None
    #     else convert it to list of single DataFrame
    if not isinstance(df, list):
        if not isinstance(df, pd.DataFrame):
            return None
        df = [df]
    # Merge list of DataFrames
    df = pd.concat(df)
    # Check if DataFrame is empty
    if df.empty:
        # On Empty DataFrame raise a warning that data not found
        if on_empty:
            raise Exception(on_empty)
        return None
    return df


def to_utc(df, field: str = '_time', frmt: str = '%Y-%m-%dT%H:%M:%S.%fZ'):
    """This function converts time field to UTC"""
    df[field] = df[field].map(lambda ts: ts.tz_convert(datetime.UTC))
    df[field] = df[field].map(lambda ts: ts.strftime(frmt))
    return df


def rename_fields(df):
    """This function renames private fields and throws redundant ones"""
    # Throw Columns
    df = df.loc[:, ~df.columns.isin(['result', 'table', '_start', '_stop'])].copy(deep=True)
    # Rename Columns
    df = df.rename(columns={'_time': 'timestamp',
                            '_field': 'field',
                            '_value': 'value',
                            '_measurement': 'measurement'})
    return df


def auto_type(df, field: str = '_value'):
    """This function converts automatically values types of a column per row"""
    df[field] = df[field].map(lambda x: ast.literal_eval(x))
    return df


def spread_fields(df, field: str = '_field', value: str = '_value', keep: list = []):
    """This function spreads colums to seperate rows"""
    colums = [col for col in df.columns if col not in keep]
    # Covert columns to rows
    df = df.melt(id_vars=keep, value_vars=colums, var_name=field, value_name=value)
    # Drop null
    df = df.dropna(how='any')
    return df


def df_int_to_float(df, measurement: str, tags: list, timestamp: str):
    """Converts integer columns to float in a DataFrame, ignoring boolean columns."""
    # Create an exclusion list from the columns that do not need to be converted
    exclude = list()
    if measurement:
        exclude.append(measurement)
    if tags:
        exclude.extend(tags)
    if timestamp:
        exclude.append(timestamp)
    # Create a list with the columns to be converted, excluding bool columns
    colums = [col for col in df.select_dtypes(include=['int']).columns if df[col].dtype != 'bool' and col not in exclude]
    # Convert selected columns to float
    for i in colums:
        df[i] = df[i].astype(float)
    return df


def df_from_db(client, bucket: str, measurement: str, fields: str | list = None, method=None, **kwargs):
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

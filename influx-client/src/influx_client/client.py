# client.py ----------------------------------------------------------------------------------------
#
# Description:
#    This script contains the client class
#
# Notes:
#    - `Bucket`:      Named location where time series data is stored. In the InfluxDB SQL implementation,
#                     a bucket is synonymous with a database. A bucket can contain multiple measurements.
#    - `Series`:      A logical grouping of data defined by shared measurement, tag and field.
#    - `Measurement`: Logical grouping for time series data. In the InfluxDB SQL implementation, a
#                     measurement is synonymous with a table. All points in a given measurement
#                     should have the same tags. A measurement contains multiple tags and fields.
#    - `Tag`:         Key-value pairs that store metadata string values for each point–for example,
#                     a value that identifies or differentiates the data source or context–for example,
#                     host, location, station, etc. Tag values may be null.
#    - `Field`:       Key-value pairs that store data for each point–for example, temperature,
#                     pressure, stock price, etc. Field values may be null, but at least one field
#                     value is not null on any given row.
#    - `Timestamp`:   Timestamp associated with the data. When stored on disk and queried, all data
#                     is ordered by time. In InfluxDB, a timestamp is a nanosecond-scale unix
#                     timestamp in UTC. A timestamp is never null.
#    - `Point`:       Similar to SQL row.
#
#    For example, a SQL table `SmartMeter`:
#    | asset_id | energy | power | temp  | timestamp                |
#    | -------- | -------| ----- | ----- | ------------------------ |
#    | 0000011  |        | 76.2  |       | 2024-02-27T01:12:59.000Z |
#    | 0000011  | 120.3  |       |       | 2024-02-28T01:14:52.000Z |
#    | 0000012  |        |       | 0.0   | 2024-02-27T01:12:59.000Z |
#    | 0000014  |        |       | 20.5  | 2024-02-28T01:14:52.000Z |
#    | 0000011  | 3.56   |       |       | 2024-02-27T01:12:59.000Z |
#    | 0000031  |        | 56.1  |       | 2024-02-28T01:14:52.000Z |
#    The columns `asset_id` are indexed.
#
#    In line-protocol format:
#    SmartMeter,asset_id=A0100 power=120.2,energy=123.1,temp=150 2024-02-27T01:12:59.000Z
#
#    Hence:
#    * Bucket     : `Forecast`
#    * Measurement: `SmartMeter`
#    * Tags       : `asset_id` = '0000011'
#    * Fields     : `power` = 76.2, `energy` = 120.3
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import re
import logging
import datetime
import warnings
from typing import Iterable
# Installed
import pandas as pd
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS, WriteOptions
from influxdb_client.client.warnings import MissingPivotFunction
# Custom
from influx_client.utilities import df_int_to_float


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)
warnings.simplefilter("ignore", MissingPivotFunction)


# ==================================================================================================
# Constants
# ==================================================================================================
#
DATE_IN_ISO = '%Y-%m-%dT%H:%M:%S.%fZ'
BATCH_SIZE = 500         # The number of data pointx to collect in a batch
FLUSH_INTERVAL = 10_000  # The number of milliseconds before the batch is written
JITTER_INTERVAL = 2_000  # The number of milliseconds to increase the batch flush interval by a
# random amount
RETRY_INTERVAL = 5_000   # The number of milliseconds to retry unsuccessful write. The retry interval
# is used when the InfluxDB server does not specify “Retry-After” header.


# ==================================================================================================
# Classes
# ==================================================================================================
#
class InfluxPoint(Point):
    """Extends InfluxDB Point."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class InfluxConverter:
    """Converts data to InfluxDB Point.

    Parameters
    ----------
    measurements (str): The measurement name.
    tags (dict): A dictionary containing tag keys and values.
    fields (dict): A dictionary containing field keys and values.
    time (str): The timestamp of the data point.
    """

    def __init__(self, measurements: str, tags: dict, fields: dict, time: str):
        self.measurements = measurements
        self.tags = tags
        self.fields = fields
        self.time = time

    def toPoint(self, force_float: bool = True) -> InfluxPoint:
        """Converts data to InfluxDB point.

        Returns
        -------
        InfluxPoint: An InfluxDB Point object.
        """
        point = InfluxPoint(self.measurements)
        for tag, value in self.tags.items():
            point = point.tag(tag, value)
        for field, value in self.fields.items():
            # Convert integers values to float by default
            if isinstance(value, int) and not isinstance(value, bool) and force_float:
                value = float(value)
            point = point.field(field, value)
        return point.time(self.time)


class InfluxQuery:
    """A class to build InfluxDB queries.

    Parameters
    ----------
    bucket (str): The name of the bucket containing the data.
    """

    def __init__(self, bucket: str):
        self._query = ''
        self._bucket = None
        self._measurement = None
        self._tags = list()
        self._fields = list()
        self._method = None
        self.bucket(bucket)

    def _statement(self, key: str, value: str | int | float):
        """Building the statement string"""
        return f'r["{key}"] == "{value}"'

    def _pattern(self, key: str, value: str | int | float):
        """Building the pattern string"""
        return f'r["{key}"] =~ /{value}/'

    def _or(self, key: str, values: list):
        """Concatenate statement strings with or"""
        return ' or '.join([self._statement(key, value) for value in values])

    def _pipe(self, key: str, values: list):
        """Concatenate statement strings with pattern"""
        return self._pattern(key, '|'.join(values))

    def _filter(self, by: str):
        """Build the filtering string"""
        return f'\n |> filter(fn: (r) => {by})'

    def bucket(self, bucket: str):
        """Set the bucket name.

        Parameters
        ----------
        bucket (str): The name of the bucket to store the data.
        """
        self._bucket = bucket
        self._query = f'from(bucket:"{bucket}")'
        return self

    def range(self, start: str = None, end: str = None, past: str = None):
        """Set the time range for the query.

        Parameters
        ----------
        start (str[optional]): The start time of the range in ISO format.
        end (str[optional]): The end time of the range in ISO format.
        past (str[optional]): Specify a past time range in minutes, hours or days (e.g., '5h', '1h', '7d').
        """
        if past:
            # Match pattern of [0-9]m, [0-9]h or [0-9]d
            match = re.search(r'(\d+)([mhd])', past)
            if match:
                number, unit = int(match.group(1)), match.group(2)
                self._query += f'\n |> range(start: -{number}{unit})'
            else:
                minutes = 525600  # Default to 1 year if not found
                self._query += f'\n |> range(start: -{minutes}m)'
        else:
            include_stop = False if end is None else True
            end = datetime.datetime.strptime(end, DATE_IN_ISO) + datetime.timedelta(milliseconds=1)\
                if end else datetime.datetime.now(datetime.UTC)
            start = datetime.datetime.strptime(start, DATE_IN_ISO) if start else end - datetime.timedelta(minutes=525600)
            start = start.strftime(DATE_IN_ISO)
            end = end.strftime(DATE_IN_ISO)
            if include_stop:
                self._query += f'\n |> range(start: {start}, stop: {end})'
            else:
                self._query += f'\n |> range(start: {start})'
        return self

    def measurement(self, measurement: str):
        """Set the measurement name.

        Parameters
        ----------
        measurement (str): The name of the measurement.
        """
        self._measurement = measurement
        self._query += self._filter(self._statement('_measurement', measurement))
        return self

    def filter(self, fields: str | list):
        """Filter data based on fields.

        Parameters
        ----------
        fields (str | list[str]): A list of fields values to filter by.
        """
        if not isinstance(fields, list):
            fields = [fields]
        self._fields = fields
        self._query += self._filter(self._or('_field', fields))
        return self

    def tags(self, **kwargs):
        """Filter data based on tag-value pairs.

        Parameters
        ----------
        **kwargs : tag-value pairs to filter by.
        """
        self._tags = list()
        for field, values in kwargs.items():
            if not isinstance(values, list):
                values = [values]
            self._tags.append(field)
            # HACK: if values length is larger than 169 it breaks the query, so use pattern instead
            if len(values) > 150:
                self._query += self._filter(self._pipe(field, values))
            else:
                self._query += self._filter(self._or(field, values))
        return self

    def method(self, method: str):
        """Set the aggregation method.

        Parameters
        ----------
        method (str): The aggregated method to perform prior to fetching the data.
        """
        self._method = method
        self._query += f'\n |> yield(name: "{method}")'
        return self

    def integral(self, column: str = '_value', unit: str = '1h', time_column: str = '_time', interpolate: str = ''):
        """Computes the area under the curve per unit of time of subsequent non-null records.

        Parameters
        ----------
        column (optional[str]): Column to operate on. Default is _value.
        interpolate (optional[str]): Type of interpolation to use. Default is "".
        timeColumn (optional[str]): Column that contains time values to use in the operation. Default is _time.
        unit (optional[str]): Unit of time to use to compute the integral.
        """
        self._query += f'\n |> integral(column: "{column}", unit: {unit}, interpolate: "{interpolate}", timeColumn: "{time_column}")'
        return self

    def aggregate_window(self, every: str = '1h', fn: str = 'mean', create_empty: bool = True,
                         time_source: str = '_stop'):
        """Downsamples data by grouping data into fixed windows of time and applying an aggregate
           or selector function to each window.

        Parameters
        ----------
        every: (str): Duration of time between windows.
        fn: (str): Aggregate or selector function to apply to each time window.
        createEmpty (optional[str]): Create empty tables for empty window. Default is true.
        column (optional[str]): Column to operate on. TODO: Add it when needed
        location (optional[str]): Location used to determine timezone. Default is the location option. TODO: Add it when needed
        offset (optional[str]): Duration to shift the window boundaries by. Default is 0s. TODO: Add it when needed
        period (optional[str]): Duration of windows. Default is the every value. TODO: Add it when needed
        timeDst (optional[str]): Column to store time values for aggregate values in. Default is _time. TODO: Add it when needed
        timeSrc (optional[str]): Column to use as the source of the new time value for aggregate values. Default is _stop.
        """

        self._query += (f'\n |> aggregateWindow(every: {every}, fn: {fn},'
                        f' createEmpty: {str(create_empty).lower()}, timeSrc: "{time_source}")')
        return self

    def window(self, every: str = '1h'):
        """Groups records using regular time intervals.

        Parameters
        ----------
        every: (str): Duration of time between windows.
        """
        self._query += f'\n |> window(every: {every})'
        return self

    def fill(self, use_previous: bool = False):
        """Replaces all null values in input tables with a non-null value.

        Parameters
        ----------
        use_previous (bool): Replace null values with the previous non-null value. Default is false.
        """
        self._query += f'\n |> fill(usePrevious: {str(use_previous).lower()})'
        return self

    def last(self):
        """Returns the last row with a non-null value from each input table."""
        self._query += f'\n |> last()'
        return self

    def first(self):
        """Returns the first non-null record from each input table."""
        self._query += f'\n |> first()'
        return self

    def limit(self, n: int = 10, offset: int = 0):
        """Limits each output table to the first n rows.

        Parameters
        ----------
        n (int): Maximum number of rows to output.
        offset (int): Number of records to skip from the start of a table before limiting to n. Default is 0.
        """
        self._query += f'\n |> limit(n: {n}, offset: {offset})'
        return self

    def tail(self, n: int, offset: int = 0):
        """Limits each output table to the last n rows.

        Parameters
        ----------
        n (int): Maximum number of rows to output.
        offset (int): Number of records to skip at the end of a table before limiting to n. Default is 0.
        """
        self._query += f'\n |> tail(n: {n}, offset: {offset})'
        return self

    def sort(self, columns: list = ['_time'], desc: bool = False):
        """Orders rows in each input table based on values in specified columns.

        Parameters
        ----------
        columns (list): List of columns to sort by. Default is ["_time"]. Sort precedence is determined by list order (left to right).
        mode (str): Sort results in descending order. Default is false.
        """
        _ = ', '.join([f'"{col}"' for col in columns])
        self._query += f'\n |> sort(columns: [{_}], desc: {str(desc).lower()})'
        return self

    def sum(self, column: str = '_value'):
        """Returns the sum of non-null values in a specified column.

        Parameters
        ----------
        column (str): Column to operate on. Default is _value.
        """
        self._query += f'\n |> sum(column: "{column}")'
        return self

    def toFloat(self):
        """Converts all values in the _value column to float types."""
        self._query += f'\n |> toFloat()'
        return self

    def toString(self):
        """Converts all values in the _value column to string types."""
        self._query += f'\n |> toString()'
        return self

    def group(self, columns: list = [], mode: str = None):
        """Defines the group key for output tables, i.e. grouping records based on values for specific columns.

        Parameters
        ----------
        columns (list): The list of columns to include or exclude (depending on the mode) in the grouping operation.
        mode (str): The method used to define the group and resulting group key. Possible values include by and except.
        """
        _ = ', '.join([f'"{col}"' for col in columns])
        mode = mode if mode in ['by', 'except'] else 'by'
        self._query += f'\n |> group(columns: [{_}], mode: "{mode}")'
        return self

    def pivot(self):
        """Pivot data specific columns."""
        self._query += f'\n |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        return self

    def query(self):
        """Build the final InfluxDB query.

        Returns
        -------
        str: The constructed query.
        """
        if self._bucket is None:
            raise Exception('Bucket not given!')
        if self._measurement is None:
            raise Exception('Measurement not given!')
        logger.debug(self._query)
        return self._query


class InfluxClient:
    """A class to interact with InfluxDB.

    Parameters
    ----------
    url (str): The URL of the InfluxDB instance.
    token (str): The token for authentication.
    org (str): The organization name.
    mode (optional[str]): The mode of operation (synchronous or asynchronous).
    debug (optional[bool]): Enable debug mode.
    """

    def __init__(self, url, token, org, mode='synchronous', debug=False):
        self.url = url
        self.token = token
        self.org = org
        self.mode = mode

        self.client = InfluxDBClient(url=url,
                                     token=token,
                                     org=org,
                                     debug=debug)
        if mode == 'synchronous':
            write_options = SYNCHRONOUS
        elif mode == 'asynchronous':
            write_options = ASYNCHRONOUS
        else:
            write_options = WriteOptions(batch_size=BATCH_SIZE,
                                         flush_interval=FLUSH_INTERVAL,
                                         jitter_interval=JITTER_INTERVAL,
                                         retry_interval=RETRY_INTERVAL)
        self.write_api = self.client.write_api(write_options=write_options)
        self.query_api = self.client.query_api()

    # -------------------------------------
    # Query
    # -------------------------------------
    def query(self, bucket: str, measurement: str, fields: str | list = None, method: str = None,
              start: str = None, end: str = None, past: str = None, integral: dict = None,
              aggregate_window: dict = None, window: dict = None, fill: dict = None,
              last: bool = False, first: bool = False, limit: dict = None, tail: dict = None,
              sort: dict = None, toFloat: bool = False, toString: bool = False, group: dict = None,
              pivot: bool = False, sum: dict = None, **tags):
        """ Build and execute a query.

        Parameters
        ----------
        bucket (str): The name of the bucket to query.
        measurement (str): The name of the measurement to query.
        fields (str | list[str]): A string or a list of string containing fields to filter by.
        method (str): The aggregation method to use.
        start (optional[str]): The start time of the query range in ISO format.
        end (optional[str]): The end time of the query range in ISO format.
        past (optional[str]): Specify a past time range in minutes or days (e.g., '1h', '7d').
        integral (optional[dict]): Computes the area under the curve per unit of time of subsequent
                                   non-null records.
        aggregate_window (optional[dict]): Downsamples data by grouping data into fixed windows of
                                           time and applying an aggregate or selector function to
                                           each window.
        window (optional[dict]): Groups records using regular time intervals.
        fill (optional[dict]): Replaces all null values in input tables with a non-null value.
        last (optional[bool]): Returns the last row with a non-null value from each input table.
        first (optional[bool]): Returns the first non-null record from each input table.
        limit (optional[dict]): Returns the first n records from each input table.
        tail (optional[dict]): Returns the last n records from each input table.
        sort (optional[dict]): Orders rows in each input table based on values in specified columns.
        toFloat (optional[bool]): Converts all values in the _value column to float types
        toString (optional[bool]): Converts all values in the _value column to string types
        group (optional[dict]): Defines the group key for output tables.
        pivot (optional[bool]): Whether to pivot the query result.
        sum (optional[dict]): Returns the sum of non-null values in a specified column.
        **tags: Additional tags as tag-key pairs to filter by.

        Returns
        -------
        str: The constructed query.

        Raises
        ------
        ValueError: If the mode is not recognized.
        """
        # Initialize the query
        q = InfluxQuery(bucket)\
            .range(start, end, past)
        if measurement:
            q = q.measurement(measurement)
        if fields:
            q = q.filter(fields)
        q = q.tags(**tags)
        if method:
            q = q.method(method)
        if window is not None:
            q = q.window(**window)
        if integral is not None:
            q = q.integral(**integral)
        if aggregate_window is not None:
            q = q.aggregate_window(**aggregate_window)
        if fill is not None:
            q = q.fill(**fill)
        if pivot:
            q = q.pivot()
        if toFloat:
            q = q.toFloat()
        if toString:
            q = q.toString()
        if group is not None:
            q = q.group(**group)
        if sort is not None:
            q = q.sort(**sort)
        if limit is not None:
            q = q.limit(**limit)
        if last:
            q = q.last()
        if first:
            q = q.first()
        if tail is not None:
            q = q.tail(**tail)
        if sum is not None:
            q = q.group()
            q = q.sum(**sum)
        return q.query()

    # -------------------------------------
    # Read
    # -------------------------------------
    def read(self, bucket: str, measurement: str, fields: str | list = None, method: str = None,
             format='line', pivot=None, **kwargs):
        """Read data from influxDB

        Parameters
        ----------
        bucket (str): The name of the bucket to stored the data
        measurement (str): The name of the 'table' that holds the data
        fields (optional[str | list[str]]): A string or a list of string containing fields to filter by.
        method (optional[str]): The aggregated method to perform prior to fetching the data
        format (str): The format of the results data
        **kwargs: Additional tags as tag-key pairs to filter by and query parameters.

        Usage examples
        --------------
        >>> .read(bucket='Forecast', measurement='SmartMeter', fields=['power', 'energy'], past='30d',
                  asset_id=['0000011'], method='mean')
        >>> .query(bucket='Forecast', measurement='SmartMeter', fields=['power', 'energy'],
                   past='30d', asset_id=['0000011'], method='mean', format='dataframe')
        >>> .query(bucket='Forecast', measurement='SmartMeter', fields=['power', 'energy'],
                   past='30d', asset_id=['0000011'], variable_id=['M1'], method='mean', format='csv')
        """
        # Control pivot argument
        if format == 'dataframe':
            # Change the default pivot value from None to True if the above statement is True
            if not any(fun in kwargs for fun in ['window', 'integral', 'interpolate', 'fill', 'last', 'first', 'tail']):
                pivot = True if pivot is None else pivot
        kwargs['pivot'] = pivot
        # Build query
        query = self.query(bucket, measurement, fields, method, **kwargs)
        if format == 'dataframe':
            return self.query_api.query_data_frame(org=self.org, query=query)
        elif format == 'csv':
            return self.query_api.query_csv(query=query, dialect=Dialect(header=False,
                                                                         delimiter=",",
                                                                         comment_prefix="#",
                                                                         annotations=[],
                                                                         date_time_format="RFC3339"))
        else:
            return self.query_api.query(org=self.org, query=query)

    # -------------------------------------
    # Write
    # -------------------------------------
    def _valid_write(self, data):
        """Valid write input is DataFrame or Extended InfluxPoint or a list of them"""
        valid_types = (pd.DataFrame, InfluxPoint)
        # Check if `data` is a single valid instance
        if isinstance(data, valid_types):
            return  True
        # Check if `data` is an iterable containing only valid instances
        if isinstance(data, Iterable) and all(isinstance(d, valid_types) for d in data):
            return  True
        # Raise an error if `data` is not a valid type or iterable of valid types
        raise TypeError('Input must be a pandas.DataFrame, InfluxPoint, or an iterable of these types.')

    def write(self, bucket: str, data, force_float: bool = True, **kwargs):
        """Write data to influxDB

        Parameters
        ----------
        bucket (str): The name of the bucket to stored the data
        data (...): One of the following formats
            1. Data Point
            2. pandas DataFrame
        force_float (bool): Disables the convertion of integers inside DataFrame to Float

        Usage examples
        --------------
        from influxdb_client import InfluxDBClient, InfluxConverter
        >>> .write('Forecast', InfluxConverter(measurements="SmartMeter",
                                               tags={"asset_id": "0000011"}
                                               fields={"power": 4.0,
                                                       "energy": 4.0},
                                               time="2024-06-01 03:00:55.581094Z)
        >>> .write('Forecast', df, data_frame_measurement_name='SmartMeter',
                                   data_frame_tag_columns=['asset_id'],
                                   data_frame_timestamp_column='timestamp')
        """
        # NOTE: Inputs was locked to specific data types to secure integer convertion to float
        self._valid_write(data)

        # Convert integers to floats, in case a DataFrame is given as data
        if isinstance(data, pd.DataFrame) and force_float:
            data = df_int_to_float(data, **kwargs)
        kwargs['data_frame_measurement_name'] = kwargs.pop('measurement', None)
        kwargs['data_frame_tag_columns'] = kwargs.pop('tags', None)
        kwargs['data_frame_timestamp_column'] = kwargs.pop('timestamp', None)
        self.write_api.write(bucket, self.org, data, **kwargs)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OneInfluxClient(InfluxClient, metaclass=Singleton):
    pass

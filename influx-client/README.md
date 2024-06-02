# Influx Client

A client that connects to [InfluxDB Service](https://github.com/GeorgeGiannopoulos/dockerfiles/tree/master/influxdb)

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

#### Upload data

```python
from influx_client import InfluxClient
client = InfluxClient(url='<INFLUX-URL>', token='<INFLUX-TOKEN>', org='<INFLUX-ORG>')
client.write('<BUCKET-NAME>', df, measurement='<MEASUREMENT-NAME>',
                                  tags=['<TAG>'],
                                  timestamp='<TIMESTAMP>')
```

#### Download data

```python
from influx_client import InfluxClient
client = InfluxClient(url='<INFLUX-URL>', token='<INFLUX-TOKEN>', org='<INFLUX-ORG>')
df = client.read(bucket=<INFLUX-BUCKET>,
                 measurement='<MEASUREMENT-NAME>',
                 fields=["<TAG>"],
                 asset_id=['<DEVICE-ID-01>',
                           '<DEVICE-ID-02>',
                           '<DEVICE-ID-03>',
                           '<DEVICE-ID-04>',
                           '<DEVICE-ID-05>',
                           '<DEVICE-ID-06>'],
                 past='30d',
                 format='dataframe')
```

## How to test

Install pytest and run

```shel
pytest
```

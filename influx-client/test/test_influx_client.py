import pytest
from influx_client import InfluxClient
from influxdb_client import InfluxDBClient

# Test data
TEST_URL = "http://localhost:8086"
TEST_TOKEN = "my_token"
TEST_ORG = "my_org"
TEST_BUCKET = "test_bucket"


@pytest.fixture
def influx_client_instance():
    return InfluxClient(TEST_URL, TEST_TOKEN, TEST_ORG)


def test_influx_client_init(influx_client_instance):
    assert influx_client_instance.url == TEST_URL
    assert influx_client_instance.token == TEST_TOKEN
    assert influx_client_instance.org == TEST_ORG


def test_influx_client_mode(influx_client_instance):
    assert influx_client_instance.mode in ['synchronous', 'asynchronous']
    assert isinstance(influx_client_instance.client, InfluxDBClient)


def test_influx_client_query(influx_client_instance):
    query = influx_client_instance.query(TEST_BUCKET, "test_measurement", [
                                         "tag1"], "mean", start='2024-01-01T00:00:00.000000Z', end='2024-01-02T00:00:00.000000Z')
    expected_query = f'from(bucket:"{TEST_BUCKET}")\n |> range(start: 2024-01-01T00:00:00.000000Z, stop: 2024-01-02T00:00:00.000000Z)' \
                     '\n |> filter(fn: (r) => r["_measurement"] == "test_measurement")' \
                     '\n |> filter(fn: (r) => r["_field"] == "tag1")' \
                     '\n |> yield(name: "mean")'
    assert query == expected_query

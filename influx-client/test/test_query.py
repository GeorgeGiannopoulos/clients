import pytest
from influx_client import InfluxQuery

# Test data
TEST_BUCKET = "test_bucket"


@pytest.fixture
def query_instance():
    return InfluxQuery(TEST_BUCKET)


def test_query_init(query_instance):
    assert query_instance._bucket is not None


def test_query_bucket(query_instance):
    assert query_instance._bucket == TEST_BUCKET
    assert query_instance._query == f'from(bucket:"{TEST_BUCKET}")'


def test_query_range(query_instance):
    query_instance.range(start='2024-01-01T00:00:00.000000Z', end='2024-01-02T00:00:00.000000Z')
    expected_query = f'from(bucket:"{TEST_BUCKET}")\n |> range(start: 2024-01-01T00:00:00.000000Z, stop: 2024-01-02T00:00:00.000000Z)'
    assert query_instance._query == expected_query


def test_query_measurement(query_instance):
    query_instance.measurement("test_measurement")
    assert query_instance._measurement == "test_measurement"
    assert query_instance._query == f'from(bucket:"{TEST_BUCKET}")\n |> filter(fn: (r) => r["_measurement"] == "test_measurement")'


def test_query_tags(query_instance):
    query_instance.tags(tag1='value1', tag2=['value2', 'value3'])
    assert query_instance._tags == ["tag1", "tag2"]
    assert query_instance._query == f'from(bucket:"{TEST_BUCKET}")\n ' + \
        '|> filter(fn: (r) => r["tag1"] == "value1")\n ' + \
        '|> filter(fn: (r) => r["tag2"] == "value2" or r["tag2"] == "value3")'


def test_query_filter(query_instance):
    query_instance.filter(['field1', 'field2'])
    assert query_instance._fields == ['field1', 'field2']
    expected_query = f'from(bucket:"{TEST_BUCKET}")\n ' +\
        '|> filter(fn: (r) => r["_field"] == "field1" or r["_field"] == "field2")'
    assert query_instance._query == expected_query


def test_query_method(query_instance):
    query_instance.method("mean")
    assert query_instance._method == "mean"
    assert query_instance._query == f'from(bucket:"{TEST_BUCKET}")\n |> yield(name: "mean")'


def test_query_pivot(query_instance):
    query_instance = query_instance.pivot()
    query_instance = query_instance.keep('field1')
    assert 'pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' in query_instance._query
    assert 'keep(columns: ["_time"])' in query_instance._query

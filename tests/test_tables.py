import gzip
import logging

import boto3
import pytest
from mypy_boto3_s3.service_resource import Bucket

from blazel.tables import SnowflakeSchema
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeTableUpsert
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import Data
from blazel.tasks import ExtractLoadJob


@pytest.fixture(scope='session')
def schema() -> SnowflakeSchema:
    columns = {
        'column0': 'varchar',
        'column1': 'datetime',
    }
    warehouse = SnowflakeWarehouse.from_serialized({
        'schema0': {
            'table0': {
                'columns': columns,
            },
            'table_csv_overwrite': {
                'columns': columns
            },
            'table_csv_upsert': {
                'columns': columns,
                'meta': {
                    'primary_key': 'column0',
                }
            },
        }
    })
    schema = warehouse['schema0']
    for table in schema:
        setattr(table, 'get_now_timestamp', lambda: '2024-01-01 00:00:00')
    yield schema


@pytest.fixture(scope='session')
def table0(schema) -> SnowflakeTable:
    return schema['table0']


def test_snowflake_table_classes(schema):
    assert isinstance(schema['table_csv_overwrite'], SnowflakeTable)
    assert isinstance(schema['table_csv_upsert'], SnowflakeTableUpsert)


def test_snowflake_load_stmt_csv(schema):
    assert schema['table_csv_overwrite'].load_stmt_str() == """\
TRUNCATE TABLE IF EXISTS sources_dev.schema0.table_csv_overwrite;
COPY INTO sources_dev.schema0.table_csv_overwrite (column0, column1)
FROM @sources_dev.public.stage/schema0/table_csv_overwrite/
FILE_FORMAT = ( 
TYPE = CSV
FIELD_DELIMITER = ';'
SKIP_BLANK_LINES = TRUE
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
 );
UPDATE sources_dev.schema0.table_csv_overwrite SET load_date='2024-01-01 00:00:00'"""


def test_snowflake_load_stmt_table_csv_upsert(schema):
    assert schema['table_csv_upsert'].load_stmt_str() == """\
DROP TABLE IF EXISTS sources_dev.schema0.table_csv_upsert_stage;
CREATE TABLE sources_dev.schema0.table_csv_upsert_stage LIKE sources_dev.schema0.table_csv_upsert;
COPY INTO sources_dev.schema0.table_csv_upsert_stage (column0, column1)
FROM @sources_dev.public.stage/schema0/table_csv_upsert/
FILE_FORMAT = ( 
TYPE = CSV
FIELD_DELIMITER = ';'
SKIP_BLANK_LINES = TRUE
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
 );
UPDATE sources_dev.schema0.table_csv_upsert_stage SET load_date='2024-01-01 00:00:00';
DELETE FROM sources_dev.schema0.table_csv_upsert
USING sources_dev.schema0.table_csv_upsert_stage
WHERE table_csv_upsert.column0 = table_csv_upsert_stage.column0;
INSERT INTO sources_dev.schema0.table_csv_upsert SELECT * FROM sources_dev.schema0.table_csv_upsert_stage"""


def test_create_table_stmt(table0):
    assert table0.create_table_stmt() == """\
DROP TABLE IF EXISTS sources_dev.schema0.table0;
CREATE TABLE sources_dev.schema0.table0 (
    column0 VARCHAR,
    column1 DATETIME,
    load_date DATETIME
)"""


@pytest.fixture(scope='session')
def snowflake_bucket(mocked_aws, parameters) -> Bucket:
    s3 = boto3.client('s3', region_name='eu-central-1')
    bucket_stem = parameters['SnowflakeStagingBucketStem']
    bucket_name = f'{bucket_stem}-dev'
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
    yield boto3.resource('s3').Bucket(bucket_name)


def test_clean_stage(monkeypatch, caplog, snowflake_bucket, table0: SnowflakeTable, parameters):
    snowflake_bucket.put_object(Key='schema0/table0/file', Body=b'test')
    with caplog.at_level(logging.INFO):
        table0.clean_stage()
        assert caplog.text.strip().endswith('Deleted 1 file(s) from s3://snowflake-staging-bucket-dev/schema0/table0/')


def test_get_key(table0):
    assert table0.get_key(1, 1) == 'schema0/table0/table0_b01_f01.csv.gz'


@pytest.fixture
def data() -> Data:
    return [
        ('a', 'b'),
        ('c', 'd'),
        ('e', 'f')
    ]


def test_get_data_bytes_1_file(caplog, table0, data):
    """
    Default batch and file size. Yields 1 file with 3 rows.
    The file is yielded after the loop completed.
    """
    csv_result = ['a;b\nc;d\ne;f\n']
    caplog.set_level(logging.DEBUG)
    data_bytes_gen = SnowflakeTable.get_data_bytes(data, max_file_size=100, csv_batch_size=100)
    data_bytes = next(data_bytes_gen)
    csv_str = gzip.decompress(data_bytes.body).decode('utf-8')
    assert csv_str == csv_result[data_bytes.file_number - 1]
    assert [r[2] for r in caplog.record_tuples] == [
        'Writing 3 rows [6 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 3 rows.',
        'Yield final data bytes (10 bytes).'
    ]


def test_get_data_bytes_2_files(caplog, table0, data):
    """
    2 rows batch size, 10 bytes file size. Yields 2 files with 2 and 1 rows.
    The second file is yielded after the loop completed.
    """
    csv_result = ['a;b\nc;d\n', 'e;f\n']
    caplog.set_level(logging.DEBUG)
    data_bytes_gen = SnowflakeTable.get_data_bytes(data, max_file_size=10, csv_batch_size=2)
    for data_bytes in data_bytes_gen:
        csv_str = gzip.decompress(data_bytes.body).decode('utf-8')
        assert csv_str == csv_result[data_bytes.file_number - 1]
    assert [r[2] for r in caplog.record_tuples] == [
        'Writing 2 rows [4 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 2 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).',
        'Writing 1 rows [2 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 1 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).'
    ]


def test_get_data_bytes_3_files(caplog, table0, data):
    """
    1 rows batch size, 10 bytes file size. Yields 3 files with 1 row.
    No file yielded after the loop completed.
    """
    csv_result = ['a;b\n', 'c;d\n', 'e;f\n']
    caplog.set_level(logging.DEBUG)
    data_bytes_gen = SnowflakeTable.get_data_bytes(data, max_file_size=10, csv_batch_size=1)
    for data_bytes in data_bytes_gen:
        csv_str = gzip.decompress(data_bytes.body).decode('utf-8')
        assert csv_str == csv_result[data_bytes.file_number - 1]
    assert [r[2] for r in caplog.record_tuples] == [
        'Writing 1 rows [2 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 1 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).',
        'Writing 1 rows [2 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 1 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).',
        'Writing 1 rows [2 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 1 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).'
    ]


def test_upload_to_stage(caplog, snowflake_bucket, table0, data):
    with caplog.at_level(logging.INFO):
        table0.upload_to_stage(data, max_file_size=10, csv_batch_size=2)
    assert [r[2] for r in caplog.record_tuples] == [
        'Writing 2 rows [4 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 2 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).',
        'Uploaded 28 bytes [2 rows] to s3://snowflake-staging-bucket-dev/schema0/table0/table0_b00_f01.csv.gz',
        'Writing 1 rows [2 entries] finished in 0.00 seconds. File buffer [10 bytes] contains 1 rows.',
        'File buffer exceeds maximum size (10 bytes). Yield data bytes (10 bytes).',
        'Uploaded 24 bytes [1 rows] to s3://snowflake-staging-bucket-dev/schema0/table0/table0_b00_f02.csv.gz',
        'Task [0] uploaded 52 bytes [2 file(s), 3 rows] to s3://snowflake-staging-bucket-dev using 100.00% of available time.'
    ]


@pytest.fixture(scope='session')
def extract_time_table(mocked_aws, parameters):
    dynamodb = boto3.client('dynamodb', region_name='eu-central-1')
    table_stem = parameters['ExtractTimeTableStem']
    table_name = f'{table_stem}-dev'
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'table_uri',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'table_uri',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )


def test_extract_task_latest_timestamp(extract_time_table, parameters, table0):
    latest_timestamp = '2024-01-01T00:00:00'
    task = ExtractLoadJob.from_table(table0).extract[0]
    assert task.options.start is None
    table0.meta.timestamp_field = 'column1'
    table0.set_latest_timestamp(latest_timestamp)
    time_range = task.get_timerange(table0)
    assert time_range.start == latest_timestamp

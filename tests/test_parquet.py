import pytest

from blazel.base import BaseSchema
from blazel.tables import SnowflakeTableOverwrite
from blazel.tables import SnowflakeTableUpsert
from blazel.parquet import SnowflakeTableParquet
from blazel.parquet import SnowflakeWarehouseParquet


@pytest.fixture(scope='session')
def schema() -> BaseSchema:
    columns = {
        'column0': 'varchar',
        'column1': 'varchar',
    }
    warehouse = SnowflakeWarehouseParquet.from_serialized({
        'schema0': {
            'table0': {
                'columns': columns
            },
            'table_csv_overwrite': {
                'columns': columns
            },
            'table_parquet_overwrite': {
                'columns': columns,
                'options': {
                    'file_format': 'parquet',
                }
            },
            'table_csv_upsert': {
                'columns': columns,
                'options': {
                    'primary_key': 'column0',
                }
            },
            'table_parquet_upsert': {
                'columns': columns,
                'options': {
                    'primary_key': 'column0',
                    'file_format': 'parquet',
                }
            }
        }
    })
    schema = warehouse['schema0']
    for table in schema:
        setattr(table, 'get_now_timestamp', lambda: '2024-01-01 00:00:00')
    yield schema


@pytest.fixture(scope='session')
def table0(schema):
    return schema['table0']


def test_snowflake_table_classes(schema):
    assert isinstance(schema['table_csv_overwrite'], SnowflakeTableOverwrite)
    assert isinstance(schema['table_parquet_overwrite'], SnowflakeTableParquet)
    assert isinstance(schema['table_parquet_overwrite'], SnowflakeTableOverwrite)
    assert isinstance(schema['table_csv_upsert'], SnowflakeTableUpsert)
    assert isinstance(schema['table_parquet_upsert'], SnowflakeTableParquet)
    assert isinstance(schema['table_parquet_upsert'], SnowflakeTableUpsert)


def test_snowflake_load_stmt_parquet_overwrite(schema):
    assert schema['table_parquet_overwrite'].load_stmt_str() == """\
TRUNCATE TABLE IF EXISTS sources_dev.schema0.table_parquet_overwrite;
COPY INTO sources_dev.schema0.table_parquet_overwrite (column0, column1) FROM (
    SELECT
    $1:column0::varchar,
    $1:column1::varchar
    FROM @sources_dev.public.stage/schema0/table_parquet_overwrite/
)
FILE_FORMAT = (
    TYPE = 'parquet'
);
UPDATE sources_dev.schema0.table_parquet_overwrite SET load_date='2024-01-01 00:00:00'"""


def test_snowflake_load_stmt_table_parquet_upsert(schema):
    assert schema['table_parquet_upsert'].load_stmt_str() == """\
DROP TABLE IF EXISTS sources_dev.schema0.table_parquet_upsert_stage;
CREATE TABLE sources_dev.schema0.table_parquet_upsert_stage LIKE sources_dev.schema0.table_parquet_upsert;
COPY INTO sources_dev.schema0.table_parquet_upsert_stage (column0, column1) FROM (
    SELECT
    $1:column0::varchar,
    $1:column1::varchar
    FROM @sources_dev.public.stage/schema0/table_parquet_upsert/
)
FILE_FORMAT = (
    TYPE = 'parquet'
);
UPDATE sources_dev.schema0.table_parquet_upsert_stage SET load_date='2024-01-01 00:00:00';
DELETE FROM sources_dev.schema0.table_parquet_upsert WHERE column0 IN (SELECT column0 FROM sources_dev.schema0.table_parquet_upsert_stage);
INSERT INTO sources_dev.schema0.table_parquet_upsert SELECT * FROM sources_dev.schema0.table_parquet_upsert_stage"""

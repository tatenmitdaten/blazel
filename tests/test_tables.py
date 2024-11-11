import os
from itertools import product

import boto3
import pytest
from moto import mock_aws

import extractload.config
from extractload.clients import Env
from extractload.tasks import ExtractLoadJob
from extractload.tasks import ScheduleTask
from extractload.wh_base import DbSchema
from extractload.wh_base import DbWarehouse
from extractload.wh_snowflake import SnowflakeTable
from tasks import ExtractFunction
from tasks import ExtractTask


@pytest.fixture
def warehouse_dict() -> dict:
    return {
        'schema1': {
            'table1': {
                'columns': {
                    'column1': 'VARCHAR',
                    'column2': {
                        'dtype': 'NUMBER',
                        'comment': 'This is a number',
                    },
                },
                'options': {
                    'primary_key': 'column1',
                }
            },
            'table2': {
                'columns': {
                    'column1': 'VARCHAR',
                    'column2': 'NUMBER',
                }
            },
        },
    }


@pytest.fixture
def warehouse_yaml() -> str:
    return """\
schema1:
  table1:
    columns:
      column1: VARCHAR
      column2:
        dtype: NUMBER
        comment: This is a number
    options:
      primary_key: column1
  table2:
    columns:
      column1: VARCHAR
      column2: NUMBER
"""


@pytest.fixture
def warehouse() -> DbWarehouse:
    n = 10
    warehouse = DbWarehouse()
    for i in range(n):
        schema_name = f'schema{i}'
        schema = DbSchema(warehouse, schema_name=schema_name)
        warehouse.schemas[schema_name] = schema
        for j in range(n):
            table_name = f'table{j}'
            schema.tables[table_name] = SnowflakeTable(schema=schema, table_name=table_name)
    yield warehouse


@pytest.fixture
def warehouse_yaml_file(warehouse_yaml, tmp_path_factory):
    fn = tmp_path_factory.mktemp("warehouse_serialized") / "img.png"
    fn.write_text(warehouse_yaml)
    os.environ['TABLES_YAML_PATH'] = str(fn)
    yield fn
    del os.environ['TABLES_YAML_PATH']


def test_warehouse_env(warehouse_dict):
    warehouse = DbWarehouse.from_serialized({})
    assert warehouse.database_name == 'sources_dev'
    warehouse.env = Env.prod
    assert warehouse.database_name == 'sources'


def test_warehouse_from_dict(warehouse_dict):
    warehouse = DbWarehouse.from_serialized(warehouse_dict)
    assert warehouse.serialized == warehouse_dict


def test_warehouse_from_yaml(warehouse_yaml):
    warehouse = DbWarehouse.from_yaml(warehouse_yaml)
    assert warehouse.as_yaml == warehouse_yaml


def test_warehouse_from_yaml_file(warehouse_yaml_file, warehouse_yaml):
    warehouse = DbWarehouse.from_yaml_file()  # uses TABLES_YAML_PATH env var
    assert warehouse.as_yaml == warehouse_yaml
    warehouse = DbWarehouse.from_yaml_file(warehouse_yaml_file)  # uses explicit path
    assert warehouse.as_yaml == warehouse_yaml


def test_warehouse_filter(warehouse):
    # get all tables from one schema
    tables = warehouse.filter(schema_names=['schema1'])
    assert all(t.schema_name == 'schema1' for t in tables)
    assert [t.table_name for t in tables] == [f'table{i}' for i in range(10)]

    # get multiple tables from multiple schemas
    schema_names = ['schema1', 'schema3', 'schema5']
    table_names = ['table1', 'table3', 'table5']
    tables = warehouse.filter(schema_names=schema_names, table_names=table_names)
    assert [(t.schema_name, t.table_name) for t in tables] == list(product(schema_names, table_names))

    # test edge cases on schema_names
    assert warehouse.filter(schema_names=['not_existing']) == []
    assert warehouse.filter(schema_names=[]) == []
    assert len(warehouse.filter()) == 100

    # test edge cases on table_names
    assert warehouse.filter(schema_names=['schema0'], table_names=['not_existing']) == []
    assert warehouse.filter(schema_names=['schema0'], table_names=[]) == []
    assert len(warehouse.filter(schema_names=['schema0'])) == 10


def test_schedule(warehouse):
    schedule_task = ScheduleTask(
        database_name='sources',
        schema_names=['schema1'],
        table_names=['table1'],
    )
    data = schedule_task.as_dict
    assert schedule_task == ScheduleTask.from_dict(data)
    schedule = schedule_task(warehouse)
    el_job = ExtractLoadJob.from_dict(schedule.as_dict['schedule'][0])
    assert el_job.clean.schema_name == 'schema1'
    assert el_job.clean.table_name == 'table1'
    assert el_job.load.schema_name == 'schema1'
    assert el_job.load.table_name == 'table1'


def test_register_extract_simple(warehouse):
    class TestExtractFunction(ExtractFunction):
        def run(self):
            print(self)
            return 'test'

    table = warehouse['schema0']['table0']
    table.register_extract_function(TestExtractFunction)
    task = ExtractTask(
        job_id='test',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    assert task(warehouse) == 'test'


@pytest.fixture(scope='session')
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"


@pytest.fixture(scope='session')
def mocked_aws(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture
def parameters():
    return {
        'JobTableStem': 'job-table',
        'TaskTableStem': 'task-table',
    }


@pytest.fixture
def task_table(mocked_aws, parameters):
    dynamodb = boto3.client('dynamodb', region_name='eu-central-1')
    table_stem = parameters['TaskTableStem']
    table_name = f'{table_stem}-dev'
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'task_id',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'task_id',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )


@pytest.fixture(scope='session')
def job_table(mocked_aws, parameters):
    dynamodb = boto3.client('dynamodb', region_name='eu-central-1')
    table_stem = parameters['JobTableStem']
    table_name = f'{table_stem}-dev'
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'job_id',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'job_id',
                'AttributeType': 'S'
            },
        ],
    )


def test_save_load_task(task_table, monkeypatch, parameters):
    monkeypatch.setattr('extractload.clients.get_parameters', lambda: parameters)
    task = ExtractTask(
        job_id='test',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    task.save()
    task.save()  # test idempotency
    task_loaded = ExtractTask.load(task.task_id)
    assert task_loaded == task

import datetime

import boto3
import pytest

from extractload.warehouse.tasks import ExtractImpl
from extractload.warehouse.tasks import ExtractLoadJob
from extractload.warehouse.tasks import ExtractTask
from extractload.warehouse.tasks import ScheduleTask
from warehouse.sf_csv import SnowflakeWarehouse
from warehouse.tasks import ExtractImplSimple
from warehouse.tasks import ExtractImplTimeRange
from warehouse.tasks import ExtractTable


@pytest.fixture
def warehouse() -> SnowflakeWarehouse:
    warehouse = SnowflakeWarehouse.from_serialized({
        'schema0': {
            'table0': {
                'columns': {}
            }
        }
    })
    yield warehouse


def test_schedule(warehouse):
    schedule_task = ScheduleTask(
        database_name='sources',
        schema_names=['schema0'],
        table_names=['table0'],
    )
    data = schedule_task.as_dict
    assert schedule_task == ScheduleTask.from_dict(data)
    schedule = schedule_task(warehouse)
    el_job = ExtractLoadJob.from_dict(schedule.as_dict['schedule'][0])
    assert el_job.clean.schema_name == 'schema0'
    assert el_job.clean.table_name == 'table0'
    assert el_job.load.schema_name == 'schema0'
    assert el_job.load.table_name == 'table0'


@pytest.fixture(scope='session')
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
        BillingMode='PAY_PER_REQUEST',
    )


def test_persist_task(task_table, parameters):
    task = ExtractTask(
        job_id='test',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    task.to_dynamodb()
    task.to_dynamodb()  # test idempotency
    task_loaded = ExtractTask.from_dynamodb(task.task_id)
    assert task_loaded == task


def test_persist_job(job_table, task_table, warehouse, parameters):
    table = warehouse['schema0']['table0']
    job = ExtractLoadJob.from_table(table)
    job.to_dynamodb()
    job.to_dynamodb()  # test idempotency
    job_loaded = ExtractLoadJob.from_dynamodb(job.job_id)
    assert job_loaded == job


def test_register_extract_simple(warehouse):
    class TestExtractImplSimple(ExtractImplSimple):
        def extract(self, limit: int = 0):
            return 'test'

    table = warehouse['schema0']['table0']
    table.register_extract_impl(TestExtractImplSimple)
    task = ExtractTask(
        job_id='test',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    assert task(warehouse) == 'test'


def test_register_extract_time_range(warehouse):
    class TestExtractImplTimeRange(ExtractImplTimeRange):
        def extract(self, start_date: datetime.datetime, end_date: datetime.datetime, limit: int = 0):
            return f'{start_date} - {end_date}'

    table: ExtractTable = warehouse['schema0']['table0']
    table.register_extract_impl(TestExtractImplTimeRange)
    task = ExtractTask(
        job_id='test',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    assert task(warehouse) == '1980-01-01 00:00:00+01:00 - 2100-12-31 00:00:00+01:00'

import datetime

import boto3
import pytest

from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import ExtractTask
from blazel.tasks import ExtractLoadTable
from blazel.tasks import ScheduleTask
from blazel.tasks import TaskOptions
from blazel.tasks import TimeRange


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
    warehouse['schema0']['table0'].options.batch_key = 'id'
    warehouse['schema0']['table0'].options.batches = 3
    schedule_task = ScheduleTask(
        database_name='sources',
        schema_names=['schema0'],
        table_names=['table0'],
        options=TaskOptions(
            limit=10
        )
    )
    data = schedule_task.as_dict
    assert schedule_task == ScheduleTask.from_dict(data)
    schedule = schedule_task(warehouse)
    el_job = ExtractLoadJob.from_dict(schedule['schedule'][0])
    assert el_job.extract[0].options == TaskOptions(limit=10, batches=3)
    assert el_job.clean.schema_name == 'schema0'
    assert el_job.clean.table_name == 'table0'
    assert el_job.load.schema_name == 'schema0'
    assert el_job.load.table_name == 'table0'


def test_schedule_empty(warehouse):
    schedule_task = ScheduleTask(
        database_name='sources',
        schema_names=[]
    )
    schedule = schedule_task(warehouse)
    assert schedule == {'schedule': []}


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
    def extract(rt: ExtractLoadTable, et: ExtractTask):
        print(rt.table_uri)
        print(et.job_id)
        return 'test'

    table: SnowflakeTable = warehouse['schema0']['table0']
    table.register_extract_function(extract)

    task = ExtractTask(
        job_id='job_id',
        database_name='sources',
        schema_name='schema0',
        table_name='table0',
    )
    assert task(warehouse) == 'test'


def test_time_range_get_batch():
    time_range = TimeRange(start='2024-01-01', end='2024-01-03')
    assert time_range.get_batch_date(0) == datetime.datetime.strptime('2024-01-01', '%Y-%m-%d')
    assert time_range.get_batch_date(2) == datetime.datetime.strptime('2024-01-03', '%Y-%m-%d')
    with pytest.raises(ValueError):
        time_range.get_batch_date(3)

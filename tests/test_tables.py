import os
from itertools import product

import pytest

from clients import Env
from extractload.tables import DbWarehouse
from extractload.tables import DbSchema
from extractload.tables import ExtractLoadJob
from extractload.tables import ScheduleTask
from extractload.tables import SnowflakeTable


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
def large_warehouse() -> DbWarehouse:
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
    warehouse = DbWarehouse.from_dict({})
    assert warehouse.database_name == 'sources_dev'
    warehouse.env = Env.prod
    assert warehouse.database_name == 'sources'


def test_warehouse_from_dict(warehouse_dict):
    warehouse = DbWarehouse.from_dict(warehouse_dict)
    assert warehouse.as_dict == warehouse_dict


def test_warehouse_from_yaml(warehouse_yaml):
    warehouse = DbWarehouse.from_yaml(warehouse_yaml)
    assert warehouse.as_yaml == warehouse_yaml


def test_warehouse_from_yaml_file(warehouse_yaml_file, warehouse_yaml):
    warehouse = DbWarehouse.from_yaml_file()  # uses TABLES_YAML_PATH env var
    assert warehouse.as_yaml == warehouse_yaml
    warehouse = DbWarehouse.from_yaml_file(warehouse_yaml_file)  # uses explicit path
    assert warehouse.as_yaml == warehouse_yaml


def test_singleton_warehouse(warehouse_dict):
    # init warehouse1
    warehouse1 = DbWarehouse()
    schema = DbSchema(warehouse1, schema_name='schema1')
    warehouse1.schemas['schema1'] = schema
    schema.tables['table1'] = SnowflakeTable(schema=schema, table_name='table1')

    # load warehouse2 from dict
    warehouse2 = DbWarehouse.from_dict(warehouse_dict)
    schema = warehouse1.schemas['schema1']

    # assert that existing tables have not been changed
    assert schema.tables['table1'].columns == {}
    # assert that new tables have been added
    assert 'table2' in schema.tables
    # assert warehouse is a singleton
    assert warehouse1 is warehouse2


def test_filter(large_warehouse):
    schema_names = ['schema1', 'schema3', 'schema5']
    table_names = ['table1', 'table3', 'table5']
    tables = large_warehouse.filter(schema_names=schema_names, table_names=table_names)
    # get multiple tables from multiple schemas
    assert [(t.schema_name, t.table_name) for t in tables] == list(product(schema_names, table_names))
    tables = large_warehouse.filter(schema_names=['schema1'])
    # get all tables from one schema
    assert all(t.schema_name == 'schema1' for t in tables)
    assert [t.table_name for t in tables] == [f'table{i}' for i in range(10)]


def test_filter_empty_args(large_warehouse):
    assert large_warehouse.filter(schema_names=['not_existing']) == []
    assert large_warehouse.filter(schema_names=[]) == []
    assert len(large_warehouse.filter()) == 100
    assert large_warehouse.filter(schema_names=['schema0'], table_names=['not_existing']) == []
    assert large_warehouse.filter(schema_names=['schema0'], table_names=[]) == []
    assert len(large_warehouse.filter(schema_names=['schema0'])) == 10


def test_schedule(warehouse_yaml_file):
    schedule_task = ScheduleTask(
        database_name='sources',
        schema_names=['schema1'],
        table_names=['table1'],
    )
    data = schedule_task.as_dict
    assert schedule_task == ScheduleTask.from_dict(data)
    el_job = ExtractLoadJob.from_dict(schedule_task.schedule[0])
    assert el_job.clean.schema_name == 'schema1'
    assert el_job.clean.table_name == 'table1'
    assert el_job.load.schema_name == 'schema1'
    assert el_job.load.table_name == 'table1'


def test_register(large_warehouse):
    def test_func(limit: int = 0):
        return 'test'

    table = large_warehouse['schema0']['table0']
    table.register_extract_function(test_func)
    task = table.create_extract_tasks()[0]
    assert task() == 'test'

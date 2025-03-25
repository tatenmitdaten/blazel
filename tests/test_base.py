import os
from itertools import product
from typing import Generator

import pytest

from blazel.clients import Env
from blazel.base import BaseSchema
from blazel.base import BaseTable
from blazel.base import BaseWarehouse


@pytest.fixture
def warehouse_dict() -> dict:
    return {
        'schema1': {
            'table1': {
                'columns': {
                    'column1': 'varchar',
                    'column2': {
                        'dtype': 'number',
                        'description': 'This is a number',
                    },
                },
                'meta': {
                    'primary_key': 'column1',
                }
            },
            'table2': {
                'columns': {
                    'column1': 'varchar',
                    'column2': 'number',
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
      column1: varchar
      column2:
        dtype: number
        description: This is a number
    meta:
      primary_key: column1
  table2:
    columns:
      column1: varchar
      column2: number
"""


@pytest.fixture
def warehouse() -> Generator[BaseWarehouse, None, None]:
    n = 10
    warehouse = BaseWarehouse()
    for i in range(n):
        schema_name = f'schema{i}'
        schema = BaseSchema(warehouse, name=schema_name)
        warehouse.schemas[schema_name] = schema
        for j in range(n):
            table_name = f'table{j}'
            schema.tables[table_name] = BaseTable(schema=schema, name=table_name)
    yield warehouse


@pytest.fixture
def warehouse_yaml_file(warehouse_yaml, tmp_path_factory):
    fn = tmp_path_factory.mktemp("warehouse_serialized") / "img.png"
    fn.write_text(warehouse_yaml)
    os.environ['TABLES_YAML_PATH'] = str(fn)
    yield fn
    del os.environ['TABLES_YAML_PATH']


def test_warehouse_env(warehouse_dict):
    wh = BaseWarehouse.from_serialized({})
    try:
        Env.set('dev')
        assert wh.database_name == 'sources_dev'
        Env.set('prod')
        assert wh.database_name == 'sources'
    finally:
        del os.environ['APP_ENV']


def test_warehouse_from_dict(warehouse_dict):
    wh = BaseWarehouse.from_serialized(warehouse_dict)
    assert wh.serialized == warehouse_dict


def test_warehouse_from_yaml(warehouse_yaml):
    wh = BaseWarehouse.from_yaml(warehouse_yaml)
    assert wh.as_yaml == warehouse_yaml


def test_warehouse_from_yaml_file(warehouse_yaml_file, warehouse_yaml):
    wh = BaseWarehouse.from_yaml_file()  # uses TABLES_YAML_PATH env var
    assert wh.as_yaml == warehouse_yaml
    wh = BaseWarehouse.from_yaml_file(warehouse_yaml_file)  # uses explicit path
    assert wh.as_yaml == warehouse_yaml


def test_warehouse_filter(warehouse):
    # get all tables from one schema
    tables = warehouse.filter(schema_names=['schema1'])
    assert all(t.schema.name == 'schema1' for t in tables)
    assert [t.name for t in tables] == [f'table{i}' for i in range(10)]

    # get multiple tables from multiple schemas
    schema_names = ['schema1', 'schema3', 'schema5']
    table_names = ['table1', 'table3', 'table5']
    tables = warehouse.filter(schema_names=schema_names, table_names=table_names)
    assert [(t.schema.name, t.name) for t in tables] == list(product(schema_names, table_names))

    # test edge cases on schema_names
    assert warehouse.filter(schema_names=['not_existing']) == []
    assert warehouse.filter(schema_names=[]) == []
    assert len(warehouse.filter()) == 100

    # test edge cases on table_names
    assert warehouse.filter(schema_names=['schema0'], table_names=['not_existing']) == []
    assert warehouse.filter(schema_names=['schema0'], table_names=[]) == []
    assert len(warehouse.filter(schema_names=['schema0'])) == 10


def test_warehouse_filter_stratify(warehouse):
    schema_names = ['schema1', 'schema3', 'schema5']
    table_names = ['table1', 'table3', 'table5']
    tables = warehouse.filter(schema_names=schema_names, table_names=table_names, stratify=True)
    assert [(t.schema.name, t.name) for t in tables] == [
        ('schema1', 'table1'),
        ('schema3', 'table1'),
        ('schema5', 'table1'),
        ('schema1', 'table3'),
        ('schema3', 'table3'),
        ('schema5', 'table3'),
        ('schema1', 'table5'),
        ('schema3', 'table5'),
        ('schema5', 'table5')
    ]

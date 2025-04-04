import logging
import re
from typing import NamedTuple

from itertools import groupby

from blazel.base import Column
from blazel.base import TableMeta
from blazel.tables import SnowflakeSchema
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeWarehouse

logger = logging.getLogger()


def get_snake_case2(name: str) -> str:
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    if name.isupper():
        return name.lower()
    return pattern.sub('_', name).lower()


def get_snake_case(name: str) -> str:
    """
    Converts a string to snake_case.

    Handles camelCase, PascalCase, and sequences of uppercase letters.
    """
    snake_case = ""
    for i, char in enumerate(name):
        # if the char is upper case and the next lower case, add an underscore before the char
        if 0 < i < len(name) - 1 and not snake_case[-1] == '_':
            if char.isupper() and name[i + 1].islower():
                snake_case += "_"
        snake_case += char.lower()
        # if the char is lower case and the next upper case, add an underscore after the char
        if 0 < i < len(name) - 1 and not snake_case[-1] == '_':
            if char.islower() and name[i + 1].isupper():
                snake_case += "_"
    return snake_case


class SQLServerTable(NamedTuple):
    schema_name: str
    table_name: str

    @property
    def table_uri(self):
        return f'{self.schema_name}.{self.table_name}'

    def warehouse_name(self, keep_db_schema: bool):
        warehouse_table_name = get_snake_case(self.table_name)
        return f'{self.schema_name.lower()}_{warehouse_table_name}' if keep_db_schema else warehouse_table_name

    def as_warehouse_table(self, schema: SnowflakeSchema, keep_db_schema: bool) -> SnowflakeTable:
        return SnowflakeTable(
            schema=schema,
            name=self.warehouse_name(keep_db_schema),
            meta=TableMeta(
                source=self.table_uri,
                stage_file_format='sqlserver_export',
            )
        )


class SQLServerColumn(NamedTuple):
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    ordinal_position: int
    numeric_precision: str
    numeric_scale: str
    is_primary_key: bool

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            schema_name=d['table_schema'],
            table_name=d['table_name'],
            column_name=d['column_name'],
            data_type=d['data_type'],
            ordinal_position=int(d['ordinal_position']),
            numeric_precision=d['numeric_precision'],
            numeric_scale=d['numeric_scale'],
            is_primary_key=d['is_primary_key'] == '1',
        )

    @property
    def table(self):
        return SQLServerTable(self.schema_name, self.table_name)

    @property
    def warehouse_name(self):
        warehouse_name = get_snake_case(self.column_name)
        if warehouse_name in ('column', 'order', 'start', 'end', 'from', 'to'):
            warehouse_name += '_'
        warehouse_name = warehouse_name.replace('__', '_')
        return warehouse_name

    @staticmethod
    def map_type(dtype: str) -> str:
        dtype_map = {
            'bit': 'int',
            'tinyint': 'int',
            'smallint': 'int',
            'char': 'varchar',
            'money': 'decimal(19,4)',
            'decimal': 'decimal(28,10)',
            'datetime2': 'datetime',
            'smalldatetime': 'datetime',
            'xml': 'variant',
            'uniqueidentifier': 'varchar',
            'varbinary': 'varchar',
            'binary': 'varchar',
        }
        return dtype_map.get(dtype, dtype)

    @property
    def warehouse_dtype(self):
        return self.map_type(self.data_type.lower())

    def as_warehouse_column(self) -> Column:
        return Column(
            name=self.warehouse_name,
            dtype=self.warehouse_dtype,
            source=self.column_name
        )


def from_sqlserver_columns(
        warehouse: SnowflakeWarehouse,
        schema_name: str,
        sqlserver_columns: list[SQLServerColumn],
        keep_db_schema: bool
) -> SnowflakeWarehouse:
    schema: SnowflakeSchema = SnowflakeSchema(warehouse=warehouse, name=schema_name)
    warehouse.add_schema(schema)
    sqlserver_columns.sort(key=lambda c: (c.table.table_uri, c.ordinal_position))
    for sqlserver_table, column_rows in groupby(sqlserver_columns, key=lambda c: c.table):
        table = sqlserver_table.as_warehouse_table(schema, keep_db_schema)
        for sqlserver_column in column_rows:
            table.add_column(sqlserver_column.as_warehouse_column())
        logger.info(f'Adding table {table.table_uri}')
        schema.add_table(table)
    return warehouse

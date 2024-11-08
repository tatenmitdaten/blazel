import os
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from pathlib import Path
from typing import TypeVar
from typing import dataclass_transform

import yaml

from config import Env

database_name_prod = os.environ.get('DATABASE_NAME_PROD', 'sources')
database_name_dev = os.environ.get('DATABASE_NAME_DEV', 'sources_dev')

TableType = TypeVar('TableType', bound='DbTable')


@dataclass
class DbColumn:
    name: str
    dtype: str
    definition: str | None = None
    source_name: str | None = None
    comment: str | None = None

    @property
    def as_dict(self) -> dict:
        # noinspection PyTypeChecker
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) != f.default
        }

    @property
    def serialized(self) -> dict | str:
        column = {k: v for k, v in self.as_dict.items() if k != 'name'}
        if len(column) == 1:
            return self.dtype
        return column

    @classmethod
    def from_serialized(cls, column_name: str, column_serialized: dict | str) -> 'DbColumn':
        if isinstance(column_serialized, str):
            return cls(
                name=column_name,
                dtype=column_serialized
            )
        return cls(
            name=column_name,
            **column_serialized
        )


@dataclass
class DbTableOptions:
    batches: int = 1
    ignore: bool = False
    look_back: int | None = None
    primary_key: str | None = None
    source_name: str | None = None
    timezone: str = 'Europe/Berlin'
    where_clause: str | None = None

    @property
    def as_dict(self) -> dict[str, int | bool | str | None]:
        # noinspection PyTypeChecker
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) != f.default
        }


@dataclass_transform()
@dataclass
class DbTable:
    schema: 'DbSchema'
    table_name: str
    columns: dict[str, DbColumn] = field(default_factory=dict)
    options: DbTableOptions = field(default_factory=DbTableOptions)

    def __iter__(self):
        return iter(self.columns.values())

    @property
    def database_name(self) -> str:
        return self.schema.warehouse.database_name

    @property
    def schema_name(self) -> str:
        return self.schema.schema_name

    @property
    def table_uri(self) -> str:
        return f'{self.database_name}.{self.schema_name}.{self.table_name}'

    @property
    def serialized(self) -> dict:
        table = {'columns': {column.name: column.serialized for column in self}}
        if self.options.as_dict:
            table['options'] = self.options.as_dict
        return table

    @classmethod
    def from_serialized(
            cls,
            schema: 'DbSchema',
            table_name: str,
            table_serialized: dict[str, dict],

    ) -> TableType:
        table = cls(
            schema=schema,
            table_name=table_name,
            options=DbTableOptions(**table_serialized.get('options', {}))
        )
        columns_serialized: dict[str, dict | str] = table_serialized['columns']
        for column_name, column_serialized in columns_serialized.items():
            table.columns[column_name] = DbColumn.from_serialized(
                column_name,
                column_serialized
            )
        return table


@dataclass
class DbSchema:
    warehouse: 'DbWarehouse'
    schema_name: str
    tables: dict[str, TableType] = field(default_factory=dict)

    def __iter__(self):
        return iter(self.tables.values())

    def __getitem__(self, item):
        try:
            return self.tables[item]
        except KeyError:
            raise KeyError(f'No table named {item}')

    def filter(self, table_names: set[str] | list[str] | None = None) -> list[TableType]:
        tables = []
        for table in self:
            if table_names is not None and table.table_name not in table_names:
                continue
            tables.append(table)
        return tables

    @property
    def serialized(self) -> dict:
        return {table.table_name: table.serialized for table in self}

    @classmethod
    def from_serialized(
            cls, warehouse: 'DbWarehouse',
            schema_name: str,
            schema_serialized: dict
    ) -> 'DbSchema':
        schema = cls(warehouse=warehouse, schema_name=schema_name)
        for table_name, table_serialized in schema_serialized.items():
            table_class = warehouse.table_class(table_serialized)
            schema.tables[table_name] = table_class.from_serialized(
                schema,
                table_name,
                table_serialized,
            )
        return schema


@dataclass
class DbWarehouse:
    env: Env = Env.get()
    schemas: dict[str, DbSchema] = field(default_factory=dict)

    def __iter__(self):
        return iter(self.schemas.values())

    def __getitem__(self, item):
        try:
            return self.schemas[item]
        except KeyError:
            raise KeyError(f'No schema named {item}')

    @property
    def database_name(self) -> str:
        if self.env == Env.prod:
            return database_name_prod
        return database_name_dev

    def table_class(self, table_serialized: dict[str, dict | None]) -> type[TableType]:
        return DbTable

    def filter(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None
    ) -> list[TableType]:
        tables: list[TableType] = []
        for schema in self:
            if schema_names is not None and schema.schema_name not in schema_names:
                continue
            tables.extend(schema.filter(table_names))
        return tables

    @property
    def serialized(self) -> dict:
        return {schema.schema_name: schema.serialized for schema in self}

    @classmethod
    def from_serialized(cls, warehouse_serialized: dict) -> 'DbWarehouse':
        warehouse = cls()
        for schema_name, schema_serialized in warehouse_serialized.items():
            warehouse.schemas[schema_name] = DbSchema.from_serialized(
                warehouse,
                schema_name,
                schema_serialized,
            )
        return warehouse

    @classmethod
    def from_yaml_file(cls, file: Path | str | None = None) -> 'DbWarehouse':
        file = file or os.environ.get('TABLES_YAML_PATH')
        if file is None:
            raise ValueError('No warehouse definition file provided')
        if isinstance(file, str):
            file = Path(file)
        if not file.exists():
            raise FileNotFoundError(f'Warehouse definition file not found: {file.absolute()}')
        with file.open() as f:
            yaml_str = f.read()
        return cls.from_yaml(yaml_str)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'DbWarehouse':
        return cls.from_serialized(yaml.safe_load(yaml_str))

    @property
    def as_yaml(self) -> str:
        return yaml.dump(self.serialized, sort_keys=False, allow_unicode=True)

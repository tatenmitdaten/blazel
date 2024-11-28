import copy
import logging
import os
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from pathlib import Path
from typing import cast
from typing import dataclass_transform
from typing import Generic
from typing import Iterator
from typing import TypeVar

import yaml

from blazel.config import Env

BaseTableType = TypeVar('BaseTableType', bound='BaseTable')
BaseSchemaType = TypeVar('BaseSchemaType', bound='BaseSchema')
BaseWarehouseType = TypeVar('BaseWarehouseType', bound='BaseWarehouse')

database_name_prod = os.environ.get('DATABASE_NAME_PROD', 'sources')
database_name_dev = os.environ.get('DATABASE_NAME_DEV', 'sources_dev')


def get_database_name():
    return database_name_prod if Env.is_prod() else database_name_dev


@dataclass
class Column:
    name: str
    dtype: str
    definition: str | None = None
    source_name: str | None = None
    comment: str | None = None

    def __post_init__(self):
        self.name = self.name
        self.dtype = self.dtype.lower()

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
    def from_serialized(cls, column_name: str, column_serialized: dict | str) -> 'Column':
        if isinstance(column_serialized, str):
            return cls(
                name=column_name,
                dtype=column_serialized
            )
        return cls(
            name=column_name,
            **column_serialized
        )


@dataclass_transform()
@dataclass
class BaseOptions:
    pass

    @property
    def as_dict(self) -> dict[str, int | bool | str | None]:
        # noinspection PyTypeChecker
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) != f.default
        }


@dataclass
class TableOptions(BaseOptions):
    ignore: bool = False
    batches: int = 1
    total_rows: int = 0
    file_format: str = 'csv'
    primary_key: str | None = None
    batch_key: str | None = None
    source_name: str | None = None
    where_clause: str | None = None
    look_back_days: int | None = None
    timestamp_field: str | None = None
    timezone: str = 'Europe/Berlin'
    file_name: str | None = None
    use_tunnel: bool = False


@dataclass_transform()
@dataclass
class BaseTable(Generic[BaseSchemaType, BaseTableType]):
    schema: BaseSchemaType
    table_name: str
    columns: dict[str, Column] = field(default_factory=dict)
    options: TableOptions = field(default_factory=TableOptions)

    def __iter__(self):
        return iter(self.columns.values())

    @property
    def database_name(self) -> str:
        # noinspection PyTypeChecker
        return self.schema.warehouse.database_name

    @property
    def schema_name(self) -> str:
        return self.schema.schema_name

    @property
    def column_names(self) -> list[str]:
        return list(self.columns.keys())

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
            cls: type[BaseTableType],
            schema: BaseSchemaType,
            table_name: str,
            table_serialized: dict[str, dict],
    ) -> BaseTableType:
        table = cls(
            schema=schema,
            table_name=table_name,
            options=TableOptions(**table_serialized.get('options', {}))
        )
        columns_serialized: dict[str, dict | str] = table_serialized.get('columns', {})
        for column_name, column_serialized in columns_serialized.items():
            table.columns[column_name] = Column.from_serialized(
                column_name,
                column_serialized
            )
        return table


@dataclass
class BaseSchema(Generic[BaseWarehouseType, BaseSchemaType, BaseTableType]):
    warehouse: BaseWarehouseType
    schema_name: str
    tables: dict[str, BaseTableType] = field(default_factory=dict[str, BaseTableType])

    def __iter__(self) -> Iterator[BaseTableType]:
        return iter(self.tables.values())

    def __getitem__(self, item) -> BaseTableType:
        try:
            return self.tables[item]
        except KeyError:
            raise KeyError(f'No table named {item}')

    def filter(self, table_names: set[str] | list[str] | None = None) -> list[BaseTableType]:
        tables = []
        if table_names is not None:
            table_names = [table_name.lower() for table_name in table_names]
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
            cls: type[BaseSchemaType],
            warehouse: BaseWarehouseType,
            schema_name: str,
            schema_serialized: dict
    ) -> BaseSchemaType:
        schema = cls(warehouse=warehouse, schema_name=schema_name)
        for table_name, table_serialized in schema_serialized.items():
            table_class: type[BaseTableType] = warehouse.table_class(table_serialized)
            schema.tables[table_name] = table_class.from_serialized(
                schema,
                table_name,
                table_serialized,
            )
        return schema


@dataclass
class BaseWarehouse(Generic[BaseWarehouseType, BaseSchemaType, BaseTableType]):
    schemas: dict[str, BaseSchemaType] = field(default_factory=dict[str, BaseSchemaType])

    def __iter__(self) -> Iterator[BaseSchemaType]:
        return iter(self.schemas.values())

    def __getitem__(self, item) -> BaseSchemaType:
        try:
            return self.schemas[item]
        except KeyError:
            raise KeyError(f'No schema named {item}')

    @property
    def database_name(self) -> str:
        return get_database_name()

    def table_class(self, table_serialized: dict[str, dict | None]) -> type[BaseTableType]:
        return cast(type[BaseTableType], BaseTable)

    def filter(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None
    ) -> list[BaseTableType]:
        tables: list[BaseTableType] = []
        if schema_names is not None:
            schema_names = [schema_name.lower() for schema_name in schema_names]
        for schema in self:
            if schema_names is not None and schema.schema_name not in schema_names:
                continue
            tables.extend(schema.filter(table_names))
        return tables

    def from_filter(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None
    ):
        warehouse = copy.deepcopy(self)
        table_uris = {table.table_uri for table in self.filter(schema_names, table_names)}
        for table in self.filter():
            if table.table_uri not in table_uris:
                del warehouse[table.schema_name].tables[table.table_name]
        for schema_name in self.schemas.keys():
            if not warehouse[schema_name].tables:
                del warehouse.schemas[schema_name]
        return warehouse

    @property
    def serialized(self) -> dict:
        return {schema.schema_name: schema.serialized for schema in self}

    @classmethod
    def from_serialized(cls: type[BaseWarehouseType], warehouse_serialized: dict) -> BaseWarehouseType:
        warehouse = cls()
        for schema_name, schema_serialized in warehouse_serialized.items():
            warehouse.schemas[schema_name] = BaseSchema.from_serialized(
                warehouse,
                schema_name,
                schema_serialized,
            )
        return warehouse

    @staticmethod
    def get_yaml_file_path(file: Path | str | None = None) -> Path:
        if isinstance(file, str):
            file = Path(file)
        if file is None:
            if 'TABLES_YAML_PATH' in os.environ:
                file = Path(os.environ['TABLES_YAML_PATH'])
            else:
                file = Path('/var/task/tables.yaml')
            logging.getLogger().info(f'Using table definition file "{file.absolute()}"')
        if not file.exists():
            raise FileNotFoundError(f'File "{file.absolute()}" not found. Cannot read table definitions.')
        return file

    @classmethod
    def from_yaml_file(cls: type[BaseWarehouseType], file: Path | str | None = None) -> BaseWarehouseType:
        file = cls.get_yaml_file_path(file)
        with file.open() as f:
            yaml_str = f.read()
        return cls.from_yaml(yaml_str)

    @classmethod
    def from_yaml(cls: type[BaseWarehouseType], yaml_str: str) -> BaseWarehouseType:
        return cls.from_serialized(yaml.safe_load(yaml_str))

    @property
    def as_yaml(self) -> str:
        return yaml.dump(self.serialized, sort_keys=False, allow_unicode=True, width=float('inf'))

    def to_yaml_file(self, file: Path | str | None = None):
        file = self.get_yaml_file_path(file)
        with file.open('w') as f:
            f.write(self.as_yaml)

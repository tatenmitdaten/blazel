import io
import logging
import os
from abc import ABC
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from pathlib import Path
from typing import Any
from typing import cast
from typing import ClassVar
from typing import dataclass_transform
from typing import Generic
from typing import Iterator
from typing import Literal
from typing import TypeVar

from blazel.config import Env
from blazel.config import yaml

BaseTableType = TypeVar('BaseTableType', bound='BaseTable')
BaseSchemaType = TypeVar('BaseSchemaType', bound='BaseSchema')
BaseWarehouseType = TypeVar('BaseWarehouseType', bound='BaseWarehouse')


def create_dict(**kwargs):
    return {key: value for key, value in kwargs.items() if value}


@dataclass
class Column:
    name: str
    dtype: str
    description: str | None = None
    source: str | None = None
    meta: dict | None = None
    tests: list[str | dict] | None = None

    def __str__(self):
        return f'{self.__class__.__name__}({self.name=},{self.dtype=},{self.source=})'

    def __post_init__(self):
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
    def dbt_format(self):
        return {
            k: v for k, v in self.as_dict.items()
            if k in ('name', 'description', 'meta', 'tests')
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
class BaseOptions(ABC):
    @property
    def as_dict(self) -> dict[str, int | bool | str | None]:
        # noinspection PyTypeChecker
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) != f.default
        }


@dataclass
class TableMeta(BaseOptions):
    ignore: bool = False
    batches: int = 1
    total_rows: int = 0
    file_format: Literal['csv', 'parquet'] = 'csv'
    primary_key: str | None = None
    timestamp_key: str | None = None
    batch_key: str | None = None
    source: str | None = None
    where_clause: str | None = None
    look_back_days: int | None = None
    timestamp_field: str | None = None
    timezone: str = 'Europe/Berlin'
    file_name: str | None = None
    use_tunnel: bool = False
    avg_row_size: int = 0
    project: str | None = None
    dataset_id: str | None = None
    location: str | None = None
    truncate: bool | None = None
    spreadsheet_id: str | None = None
    stage_file_name: str | None = None  # TODO: remove
    stage_file_format: str | None = None


@dataclass_transform()
@dataclass
class BaseTable(Generic[BaseSchemaType, BaseTableType]):
    schema: BaseSchemaType
    name: str
    description: str | None = None
    meta: TableMeta = field(default_factory=TableMeta)
    columns: dict[str, Column] = field(default_factory=dict)

    def __str__(self):
        return f'{self.__class__.__name__}(warehouse={self.schema.warehouse.name},schema={self.schema.name},{self.name=},columns={len(self.columns)})'

    def __iter__(self):
        return iter(self.columns.values())

    def __setitem__(self, key, value):
        raise NotImplementedError('Use add_column() to add a column to a table')

    def add_column(self, column: Column):
        self.columns[column.name] = column

    def drop_column(self, column_name: str):
        if column_name in self.columns:
            del self.columns[column_name]

    @property
    def database_name(self):
        return self.schema.warehouse.name

    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def column_names(self) -> list[str]:
        return list(self.columns.keys())

    @property
    def table_uri(self) -> str:
        return f'{self.database_name}.{self.schema_name}.{self.name}'

    @property
    def dbt_format(self):
        table: dict[str, str | dict | list] = {
            'name': self.name,
            'columns': [column.dbt_format for column in self],
        }
        if self.description:
            table['description'] = self.description
        if self.meta.as_dict:
            table['meta'] = self.meta.as_dict
        return table

    @property
    def serialized(self) -> dict:
        return create_dict(
            _description=self.description,
            _meta=self.meta.as_dict,
            **{column.name: column.serialized for column in self}
        )

    @classmethod
    def from_serialized(
            cls: type[BaseTableType],
            schema: BaseSchemaType,
            table_name: str,
            table_serialized: dict[str, dict[str, Any] | str],
    ) -> BaseTableType:
        meta = table_serialized.get('meta') or table_serialized.get('_meta') or {}
        table = cls(
            schema=schema,
            name=table_name,
            description=cast(str | None, table_serialized.get('_description')),
            meta=TableMeta(**cast(dict, meta))
        )
        if 'columns' in table_serialized:
            columns_serialized = cast(dict[str, Any], table_serialized['columns'])
        else:
            columns_serialized = {k: v for k, v in table_serialized.items() if k not in ('_description', '_meta')}
        for column_name, column_serialized in columns_serialized.items():
            table.columns[column_name] = Column.from_serialized(
                column_name,
                column_serialized
            )
        return table


@dataclass
class BaseSchema(Generic[BaseWarehouseType, BaseSchemaType, BaseTableType]):
    warehouse: BaseWarehouseType
    name: str
    description: str | None = None
    meta: dict | None = None
    tables: dict[str, BaseTableType] = field(default_factory=dict[str, BaseTableType])

    def __str__(self):
        return f'{self.__class__.__name__}(warehouse={self.warehouse.name},{self.name=},tables={len(self.tables)})'

    def __iter__(self) -> Iterator[BaseTableType]:
        return iter(self.tables.values())

    def __setitem__(self, key, value) -> None:
        raise NotImplementedError('Use add_table() to add a table to a schema')

    def __getitem__(self, item) -> BaseTableType:
        try:
            return self.tables[item]
        except KeyError:
            raise KeyError(f'No table named {item}')

    def add_table(self, table: BaseTableType):
        self.tables[table.name] = table

    def drop_table(self, table: BaseTableType | str):
        table_name = table.name if isinstance(table, BaseTable) else table
        if table_name in self.tables:
            del self.tables[table_name]

    def filter_tables(
            self,
            table_names: set[str] | list[str] | None = None
    ) -> list[BaseTableType]:
        tables = []
        if table_names is not None:
            table_names = [table_name.lower() for table_name in table_names]
        for table in self:
            if table_names is None:
                if table.meta.ignore:
                    continue
            else:
                if table.name not in table_names:
                    continue
            tables.append(table)
        return tables

    @property
    def dbt_format(self) -> dict:
        schema: dict[str, str | dict | list] = {
            'name': self.name,
            'database': self.warehouse.default_database_name_prod,
            'tables': [table.dbt_format for table in self],
        }
        if self.description:
            schema['description'] = self.description
        if self.meta:
            schema['meta'] = self.meta
        return schema

    @property
    def serialized(self) -> dict:
        schema = {}
        if self.description:
            schema['_description'] = self.description
        if self.meta:
            schema['_meta'] = self.meta
        for table in self:
            schema[table.name] = table.serialized
        return schema

    @classmethod
    def from_serialized(
            cls: type[BaseSchemaType],
            warehouse: BaseWarehouseType,
            schema_name: str,
            schema_serialized: dict
    ) -> BaseSchemaType:
        schema = cls(warehouse=warehouse, name=schema_name)
        for table_name, table_serialized in schema_serialized.items():
            if table_name == '_description':
                schema.description = table_serialized
                continue
            if table_name == '_meta':
                schema.description = table_serialized
                continue
            table_class: type[BaseTableType] = warehouse.table_class(table_serialized)
            schema.tables[table_name] = table_class.from_serialized(
                schema,
                table_name,
                table_serialized,
            )
        return schema


@dataclass
class BaseWarehouse(Generic[BaseWarehouseType, BaseSchemaType, BaseTableType]):
    default_database_name_prod: ClassVar[str] = 'sources'
    default_database_name_dev: ClassVar[str] = 'sources_dev'
    schemas: dict[str, BaseSchemaType] = field(default_factory=dict[str, BaseSchemaType])
    source_file: Path | None = None

    def __str__(self):
        return f'{self.__class__.__name__}({self.name=},schemas={len(self.schemas)})'

    def __iter__(self) -> Iterator[BaseSchemaType]:
        return iter(self.schemas.values())

    def __setitem__(self, key, value) -> None:
        raise NotImplementedError('Use add_schema() to add a schema to a warehouse')

    def __getitem__(self, item) -> BaseSchemaType:
        try:
            return self.schemas[item]
        except KeyError:
            raise KeyError(f'No schema named {item}')

    @property
    def name(self) -> str:
        return self.get_database_name()

    @staticmethod
    def get_database_name() -> str:
        if Env.is_prod():
            return os.environ.get('DATABASE_NAME_PROD', BaseWarehouse.default_database_name_prod)
        else:
            return os.environ.get('DATABASE_NAME_DEV', BaseWarehouse.default_database_name_dev)

    def add_schema(self, schema: BaseSchemaType):
        self.schemas[schema.name] = schema

    def drop_schema(self, schema: BaseSchemaType | str):
        schema_name = schema.name if isinstance(schema, BaseSchema) else schema
        if schema_name in self.schemas:
            del self.schemas[schema_name]

    @staticmethod
    def table_class(_: dict[str, dict | None]) -> type[BaseTableType]:
        return cast(type[BaseTableType], BaseTable)

    def filter_schemas(
            self,
            schema_names: set[str] | list[str] | None = None
    ) -> list[BaseSchemaType]:
        if schema_names is not None:
            schema_names = [schema_name.lower() for schema_name in schema_names]
        return [
            schema for schema in self
            if schema_names is None or schema.name in schema_names
        ]

    def filter(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None,
            stratify: bool = False
    ) -> list[BaseTableType]:
        warehouse = {
            schema.name: [table for table in schema.filter_tables(table_names)]
            for schema in self.filter_schemas(schema_names)
        }
        result: list[BaseTableType] = []
        while warehouse:
            for schema_name in list(warehouse.keys()):
                if stratify:
                    if warehouse[schema_name]:
                        result.append(warehouse[schema_name].pop(0))
                    if not warehouse[schema_name]:
                        del warehouse[schema_name]
                else:
                    result.extend(warehouse[schema_name])
                    del warehouse[schema_name]

        return result

    @property
    def dbt_format(self):
        return {
            'version': 2,
            'sources': [schema.dbt_format for schema in self],
        }

    @property
    def serialized(self) -> dict:
        return {schema.name: schema.serialized for schema in self}

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
                # default path in Lambda runtime
                file = Path('/var/task/tables.yaml')
            logging.getLogger().debug(f'Using table definition file "{file.absolute()}"')
        if not file.exists():
            raise FileNotFoundError(f'File "{file.absolute()}" not found. Cannot read table definitions.')
        return file

    @classmethod
    def from_yaml_file(cls: type[BaseWarehouseType], file: Path | str | None = None) -> BaseWarehouseType:
        file = cls.get_yaml_file_path(file)
        yaml_str = file.read_text()
        warehouse = cls.from_yaml(yaml_str)
        warehouse.source_file = file
        return warehouse

    @classmethod
    def from_yaml(cls: type[BaseWarehouseType], yaml_str: str) -> BaseWarehouseType:
        return cls.from_serialized(yaml.load(yaml_str))

    @property
    def as_yaml(self) -> str:
        f = io.StringIO()
        yaml.dump(self.serialized, f)
        return f.getvalue()

    def to_yaml_file(self, file: Path | str | None = None):
        if isinstance(file, str):
            file = Path(file)
        if file is None:
            file = Path(self.source_file)
        with file.open('w') as f:
            f.write(self.as_yaml)

    def to_dbt_format(self, file: Path | str):
        if isinstance(file, str):
            file = Path(file)
        f = io.StringIO()
        yaml.dump(self.dbt_format, f)
        yaml_str = f"# This file was autogenerated by `loader dbt`.\n\n{f.getvalue()}"
        file.write_text(yaml_str)

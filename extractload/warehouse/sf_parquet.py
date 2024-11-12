import io
import logging
import re
from types import ModuleType
from typing import cast
from typing import ClassVar
from typing import TYPE_CHECKING

from extractload.warehouse.base import TableType
from extractload.warehouse.sf_csv import default_stage_suffix
from extractload.warehouse.sf_csv import SnowflakeTable
from extractload.warehouse.sf_csv import SnowflakeTableOverwrite
from extractload.warehouse.sf_csv import SnowflakeTableUpsert
from extractload.warehouse.sf_csv import SnowflakeWarehouse

if TYPE_CHECKING:
    import pyarrow  # type: ignore

logger = logging.getLogger()


class SnowflakeWarehouseParquet(SnowflakeWarehouse):
    def table_class(self, table_serialized: dict[str, dict | None]) -> type[TableType]:
        options = table_serialized.get('options') or {}

        file_format = options.get('file_format', 'csv')
        if file_format == 'csv':
            return super().table_class(table_serialized)

        has_primary_key = options.get('primary_key') is not None
        if has_primary_key:
            return cast(type[TableType], SnowflakeTableParquetUpsert)
        return cast(type[TableType], SnowflakeTableParquetOverwrite)


class SnowflakeTableParquet(SnowflakeTable):
    _pyarrow: ClassVar[ModuleType | None] = None

    def _import_pyarrow(self):
        if type(self)._pyarrow is not None:
            try:
                import pyarrow
                type(self)._pyarrow = pyarrow
            except ImportError:
                raise ImportError('pyarrow is required for SnowflakeTableParquet')
        return type(self)._pyarrow

    @property
    def parquet_schema(self) -> 'pyarrow.Schema':
        pyarrow = self._import_pyarrow()
        pyarrow_fields = []
        for column in self:
            name = column.name.strip('"')
            if column.dtype == 'datetime':
                pyarrow_field = pyarrow.field(name, pyarrow.timestamp('us'))
            elif column.dtype == 'time':
                pyarrow_field = pyarrow.field(name, pyarrow.time64('us'))
            elif column.dtype == 'date':
                pyarrow_field = pyarrow.field(name, pyarrow.date32())
            elif column.dtype == 'int':
                pyarrow_field = pyarrow.field(name, pyarrow.int32())
            elif column.dtype == 'varchar':
                pyarrow_field = pyarrow.field(name, pyarrow.string())
            elif column.dtype == 'double':
                pyarrow_field = pyarrow.field(name, pyarrow.float64())
            elif column.dtype.startswith('decimal'):
                precision, scale = re.findall(r'\d+', column.dtype)
                pyarrow_field = pyarrow.field(name, pyarrow.decimal128(int(precision), int(scale)))
            else:
                raise ValueError(f'Unknown datatype {column.dtype}')
            pyarrow_fields.append(pyarrow_field)
        return pyarrow.schema(pyarrow_fields)

    def get_key(self, file_number: int = 0, suffix='parquet') -> str:
        return super().get_key(file_number, suffix)

    def rows_to_bytes(self, rows: tuple[tuple, ...]) -> bytes:
        pyarrow = self._import_pyarrow()
        with io.BytesIO() as buffer:
            parquet_writer = pyarrow.parquet.ParquetWriter(buffer, self.parquet_schema)
            try:
                # noinspection PyArgumentList
                pyarrow_table = pyarrow.Table.from_arrays(
                    arrays=[pyarrow.array(row) for row in zip(*rows)],
                    schema=self.parquet_schema
                )
            except pyarrow.lib.ArrowInvalid as e:
                logger.info(e)
                raise

            parquet_writer.write_table(pyarrow_table)
        return buffer.getvalue()

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        indent = 4 * ' '
        conv_columns_str = ',\n'.join(
            f'{indent}TO_TIMESTAMP_NTZ($1:{column.name}::INT, 6)' if column.dtype == 'datetime'
            else f'{indent}$1:{column.name}::{column.dtype}'
            for column in self
        )
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.database_name}.{self.schema_name}.{self.table_name}{suffix} ({column_names}) FROM (
            SELECT
                {conv_columns_str}
            FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        )
        FILE_FORMAT = (
            TYPE = 'parquet'
        )"""


class SnowflakeTableParquetOverwrite(SnowflakeTableParquet, SnowflakeTableOverwrite):
    pass


class SnowflakeTableParquetUpsert(SnowflakeTableParquet, SnowflakeTableUpsert):
    pass

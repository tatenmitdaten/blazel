import logging
from typing import cast

from blazel.base import BaseTableType
from blazel.tables import default_stage_suffix
from blazel.tables import INDENT
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeTableOverwrite
from blazel.tables import SnowflakeTableUpsert
from blazel.tables import SnowflakeWarehouse

logger = logging.getLogger()


class SnowflakeWarehouseParquet(SnowflakeWarehouse):
    def table_class(self, table_serialized: dict[str, dict | None]) -> type[BaseTableType]:
        options = table_serialized.get('options') or {}

        file_format = options.get('file_format', 'csv')
        if file_format == 'csv':
            return super().table_class(table_serialized)

        has_primary_key = options.get('primary_key') is not None
        if has_primary_key:
            return cast(type[BaseTableType], SnowflakeTableParquetUpsert)
        return cast(type[BaseTableType], SnowflakeTableParquetOverwrite)


class SnowflakeTableParquet(SnowflakeTable):

    def copy_table_stmt(self, suffix: str = '') -> str:
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
        )""".replace(INDENT, '')


class SnowflakeTableParquetOverwrite(SnowflakeTableParquet, SnowflakeTableOverwrite):
    pass


class SnowflakeTableParquetUpsert(SnowflakeTableParquet, SnowflakeTableUpsert):
    pass

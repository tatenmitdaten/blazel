import csv
import datetime
import gzip
import io
import logging
import re
from itertools import batched
from pathlib import Path
from types import ModuleType
from typing import ClassVar
from typing import Generator
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError

from extractload.clients import get_snowflake_secret
from extractload.clients import get_snowflake_staging_bucket
from extractload.config import default_timestamp_format
from extractload.config import default_timezone
from extractload.tasks import Data
from extractload.tasks import ExtractTable
from extractload.wh_base import DbWarehouse
from extractload.wh_base import TableType

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger()

default_stage_suffix = '_stage'


def get_private_key_bytes(private_key_pem_string: str) -> bytes:
    private_key = serialization.load_pem_private_key(
        private_key_pem_string.encode(),
        password=None,
        backend=default_backend()
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def get_snowflake_connection(database: str) -> SnowflakeConnection:
    secret = get_snowflake_secret()
    conn_dict = {
        key: value for key, value in secret.items()
        if key in ('account', 'user', 'warehouse', 'role')
    }
    return connect(
        **conn_dict,
        private_key=get_private_key_bytes(
            private_key_pem_string=secret['private_key']
        ),
        database=database
    )


class SnowflakeWarehouse(DbWarehouse):

    def table_class(self, table_serialized: dict[str, dict | None]) -> type[TableType]:
        has_primary_key = table_serialized.get('options', {}).get('primary_key') is not None
        file_format = table_serialized.get('options', {}).get('file_format', 'csv')
        match has_primary_key, file_format:
            case True, 'csv':
                return SnowflakeTableUpsert
            case True, 'parquet':
                return SnowflakeTableParquetUpsert
            case False, 'csv':
                return SnowflakeTableOverwrite
            case False, 'parquet':
                return SnowflakeTableParquetOverwrite

    def create_tables(
            self,
            schema_names: set[str] | None = None,
            table_names: set[str] | None = None,
            overwrite: bool = False,
            save_files: bool = False
    ):
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            with snowflake_conn.cursor() as snowflake_cursor:
                for table in self.filter(schema_names, table_names):
                    create_stmt = table.create_table_stmt()
                    if save_files:
                        file_path = Path('sql') / str(table.schema_name) / f'{table.table_name}.sql'
                        with file_path.open('w', encoding='utf-8') as f:
                            f.write(create_stmt)
                    drop_stmt, create_stmt = create_stmt.split(';')
                    if overwrite:
                        snowflake_cursor.execute(drop_stmt)
                        logger.info(f'Dropped {table.table_uri}.')
                    try:
                        snowflake_cursor.execute(create_stmt)
                        logger.info(f'Created {table.table_uri}.')
                    except ProgrammingError as e:
                        if e.errno == 2002:
                            logger.info(f'Table {table.table_uri} exists. Skipping.')
                        else:
                            raise


class SnowflakeTable(ExtractTable):

    def create_table_stmt(self) -> str:
        def f_comment(comment: str | None) -> str:
            return f" COMMENT '{comment}'" if comment else ''

        columns = ',\n'.join(
            [f'\t{column.name} {column.dtype.upper()}{f_comment(column.comment)}'
             for column in self] + ['\tload_date DATETIME']
        )
        return f"DROP TABLE IF EXISTS {self.table_uri};\n\n" \
               f"CREATE TABLE {self.table_uri} (\n{columns}\n)"

    def clean_stage(self):
        prefix = f'{self.schema_name}/{self.table_name}/'
        objects, counter = [], 0
        bucket = get_snowflake_staging_bucket()
        for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
            counter += 1
            objects.append({'Key': obj.key})
            if len(objects) >= 1000:
                bucket.delete_objects(Delete={
                    'Objects': objects
                })
                objects = []
        if objects:
            bucket.delete_objects(Delete={'Objects': objects})
        path = f's3://{bucket.name}/{prefix}'
        logger.info(f'Deleted {counter} file(s) from {path}')

    def get_key(self, file_number: int = 0) -> str:
        raise NotImplementedError

    def rows_to_bytes(self, rows_dict: tuple[tuple, ...]) -> bytes:
        raise NotImplementedError

    def get_rows(self, data: Data) -> Generator[tuple, None, None]:
        for row in data:
            yield tuple(row.get(column.name) for column in self)

    def add_extract_ts(self, data: Data) -> Data:
        if 'utc_extract_ts' in self.columns:
            utc_extract_ts = datetime.datetime.now(datetime.UTC).strftime(default_timestamp_format)
            for row in data:
                row['utc_extract_ts'] = utc_extract_ts
                yield row
        else:
            yield from data

    def upload_to_stage(self, data: Data, chunk_size: int = 500_000):
        total_rows = 0
        bucket = get_snowflake_staging_bucket()
        rows = self.get_rows(self.add_extract_ts(data))
        for file_number, batch in enumerate(batched(rows, chunk_size)):
            total_rows += len(batch)
            key = self.get_key(file_number)
            body = gzip.compress(self.rows_to_bytes(batch))
            bucket.put_object(Body=body, Key=key)
            logger.info(f'Uploaded {len(body)} bytes to s3://{bucket.name}/{key}')
        logger.info(f'Uploaded {total_rows} rows to stage')

    def update_load_date_stmt(self, suffix: str = '') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        timestamp = datetime.datetime.now(ZoneInfo(default_timezone)).strftime(default_timestamp_format)
        return f"""\
        UPDATE {self.table_uri}{suffix} SET load_date='{timestamp}'
        """

    def staging_table_stmt(self) -> str:
        return f"""\
        DROP TABLE IF EXISTS {self.table_uri}{default_stage_suffix};
        CREATE TABLE {self.table_uri}{default_stage_suffix} LIKE {self.table_uri}
        """

    def copy_table_stmt(self) -> str:
        raise NotImplementedError

    def load_stmt(self) -> str:
        raise NotImplementedError

    def load_from_stage(self):
        """
        Load warehouse_serialized from Snowflake stage into target table.
        """
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            logger.info(f'Connected to Snowflake account {snowflake_conn.account}.')
            with snowflake_conn.cursor() as snowflake_cursor:
                for stmt in self.load_stmt().split(';'):
                    if stmt.strip():
                        logger.info(stmt)
                        snowflake_cursor.execute(stmt)
                results = [row[0] for row in snowflake_cursor.fetchall()]
                logger.info(f'Loaded {results} rows into "{self.table_uri}" in parallel')


class SnowflakeTableCsv(SnowflakeTable):

    def get_key(self, file_number: int = 0) -> str:
        file_name = f'file_{file_number:02d}'
        return f'{self.schema_name}/{self.table_name}/{file_name}.csv.gz'

    def rows_to_bytes(self, rows_dict: tuple[tuple, ...]) -> bytes:
        with io.StringIO() as csv_file:
            writer = csv.writer(
                csv_file,
                delimiter=';',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator='\n',
                extrasaction='ignore'
            )
            writer.writerow(column.name for column in self)
            for row in rows_dict:
                writer.writerow(row)
            return csv_file.getvalue().encode('utf-8')

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.table_uri}{suffix} ({column_names})
        FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        FILE_FORMAT = (
            TYPE = 'csv'
            FIELD_DELIMITER = ';'
            EMPTY_FIELD_AS_NULL = TRUE
            SKIP_HEADER = 1
            SKIP_BLANK_LINES = TRUE
            TRIM_SPACE = TRUE,
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )"""


class SnowflakeTableParquet(SnowflakeTable):
    _pyarrow: ClassVar[ModuleType | None] = None

    def __post_init__(self):
        try:
            import pyarrow
            type(self)._pyarrow = pyarrow
        except ImportError:
            raise ImportError('pyarrow is required for SnowflakeTableParquet')

    @property
    def parquet_schema(self) -> 'pyarrow.Schema':
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

    def get_key(self, file_number: int = 0) -> str:
        file_name = f'file_{file_number:02d}'
        return f'{self.schema_name}/{self.table_name}/{file_name}.parquet'

    def rows_to_bytes(self, rows: tuple[tuple, ...]) -> bytes:
        with io.BytesIO() as buffer:
            parquet_writer = self._pyarrow.parquet.ParquetWriter(buffer, self.parquet_schema)
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
        conv_columns_str = ',\n'.join(
            f'\tTO_TIMESTAMP_NTZ($1:{column.name}::INT, 6)' if column.dtype == 'datetime'
            else f'\t$1:{column.name}::{column.dtype}'
            for column in self
        )
        column_names = ', '.join(column.name for column in self)
        return f"""
        COPY INTO {self.database_name}.{self.schema_name}.{self.table_name}{suffix} ({column_names}) FROM (
            SELECT {conv_columns_str}
            FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        )
        FILE_FORMAT = (
            TYPE = 'parquet'
        )"""


class SnowflakeTableOverwrite(SnowflakeTableCsv):

    def truncate_table_stmt(self) -> str:
        return f"""\
        TRUNCATE TABLE IF EXISTS {self.table_uri}
        """

    def load_stmt(self) -> str:
        logger.info(f'Replace warehouse_serialized in {self.table_uri}...')
        return ';\n'.join([
            self.truncate_table_stmt(),
            self.copy_table_stmt(),
            self.update_load_date_stmt(),
        ]).replace(8 * ' ', ' ')


class SnowflakeTableUpsert(SnowflakeTableCsv):

    def __post_init__(self):
        if self.options.primary_key is None:
            raise ValueError(f'primary_key is required for SnowflakeUpsertTable "{self.table_uri}"')

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.table_uri}{suffix} ({column_names})
        FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        FILE_FORMAT = (FORMAT_NAME = {self.database_name}.public.csv_file)"""

    def delete_append_table_stmt(self) -> str:
        return f"""
        DELETE FROM {self.table_uri} WHERE {self.options.primary_key} IN (SELECT {self.options.primary_key} FROM {self.table_uri}{default_stage_suffix});
        INSERT INTO {self.table_uri} SELECT * FROM {self.table_uri}{default_stage_suffix}"""

    def load_stmt(self) -> str:
        logger.info(f'Merging into {self.table_uri} using primary key {self.options.primary_key}...')
        return ';\n'.join([
            self.staging_table_stmt(),
            self.copy_table_stmt(default_stage_suffix),
            self.update_load_date_stmt(default_stage_suffix),
            self.delete_append_table_stmt(),
        ]).replace(8 * ' ', ' ')


class SnowflakeTableParquetUpsert(SnowflakeTableUpsert, SnowflakeTableParquet):
    pass


class SnowflakeTableParquetOverwrite(SnowflakeTableOverwrite, SnowflakeTableParquet):
    pass

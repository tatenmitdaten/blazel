import csv
import datetime
import gzip
import io
import logging
from dataclasses import field
from enum import Enum
from itertools import batched
from pathlib import Path
from typing import cast
from typing import Generator
from typing import TypeVar
from zoneinfo import ZoneInfo

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import connect
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from blazel.clients import get_extract_time_table
from blazel.clients import get_snowflake_secret
from blazel.clients import get_snowflake_staging_bucket
from blazel.config import default_timestamp_format
from blazel.config import default_timezone
from blazel.tasks import Data
from blazel.tasks import ExtractLoadSchema
from blazel.tasks import ExtractLoadTable
from blazel.tasks import TableTaskType
from blazel.tasks import ExtractLoadWarehouse

logger = logging.getLogger()
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

default_stage_suffix = '_stage'

SnowflakeTableType = TypeVar('SnowflakeTableType', bound='SnowflakeTable')
SnowflakeSchemaType = TypeVar('SnowflakeSchemaType', bound='SnowflakeSchema')
SnowflakeWarehouseType = TypeVar('SnowflakeWarehouseType', bound='SnowflakeWarehouse')

INDENT = 8 * ' '


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


class SQL(Enum):
    drop = 'DROP'
    create = 'CREATE'
    truncate = 'TRUNCATE'
    delete = 'DELETE'
    copy = 'COPY'
    update = 'UPDATE'
    insert = 'INSERT'


class SnowflakeTable(ExtractLoadTable[SnowflakeSchemaType, SnowflakeTableType, TableTaskType]):

    def create_table_stmt(self) -> str:
        def f_comment(comment: str | None) -> str:
            return f" COMMENT '{comment}'" if comment else ''

        indent = 4 * ' '
        columns = ',\n'.join(
            [f'{indent}{column.name} {column.dtype.upper()}{f_comment(column.comment)}'
             for column in self] + [f'{indent}load_date DATETIME']
        )
        return f"DROP TABLE IF EXISTS {self.table_uri};\n" \
               f"CREATE TABLE {self.table_uri} (\n{columns}\n)"

    def clean_stage(self) -> dict | None:
        prefix = f'{self.schema_name}/{self.table_name}/'
        objects, counter = [], 0
        bucket = get_snowflake_staging_bucket()
        for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
            counter += 1
            objects.append({'Key': obj.key})
            if len(objects) >= 1000:
                bucket.delete_objects(Delete={'Objects': objects})  # type: ignore
                objects = []
        if objects:
            bucket.delete_objects(Delete={'Objects': objects})  # type: ignore
        path = f's3://{bucket.name}/{prefix}'
        logger.info(f'Deleted {counter} file(s) from {path}')
        return None

    def get_key(self, batch_number: int, file_number: int, suffix: str = 'csv.gz') -> str:
        file_name = f'{self.table_name}_b{batch_number:02d}_f{file_number:02d}.{suffix}'
        return f'{self.schema_name}/{self.table_name}/{file_name}'

    def rows_to_bytes(self, rows: tuple[tuple, ...]) -> bytes:
        with io.StringIO() as csv_file:
            writer = csv.writer(
                csv_file,
                delimiter=';',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator='\n',
            )
            writer.writerow(column.name for column in self)
            for row in rows:
                writer.writerow(row)
            csv_str = csv_file.getvalue()
        return gzip.compress(csv_str.encode('utf-8'))

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.table_uri}{suffix} ({column_names})
        FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        FILE_FORMAT = ( TYPE = CSV FIELD_DELIMITER = ';' EMPTY_FIELD_AS_NULL = TRUE SKIP_HEADER = 1 SKIP_BLANK_LINES = TRUE TRIM_SPACE = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"' )""".replace(
            INDENT, '')

    def get_rows(self, data: Data) -> Generator[tuple, None, None]:
        for row in data:
            yield tuple(row.get(column.name) for column in self)

    def upload_to_stage(self, data: Data, batch_number: int = 0, chunk_size: int = 500_000):
        def human_readable_size(obj: bytes) -> str:
            len_bytes = len(obj)
            if len_bytes < 1024:
                return f'{len_bytes} bytes'
            if len_bytes < 1024 ** 2:
                return f'{len_bytes / 1024:.2f} Kb'
            return f'{len_bytes / 1024 ** 2:.2f} Mb'

        total_rows = 0
        bucket = get_snowflake_staging_bucket()
        rows = self.get_rows(data)
        for file_number, batch in enumerate(batched(rows, chunk_size)):
            total_rows += len(batch)
            key = self.get_key(batch_number, file_number)
            body = self.rows_to_bytes(batch)
            bucket.put_object(Body=body, Key=key)
            logger.info(f'Uploaded {human_readable_size(body)} ({len(batch)} rows) to s3://{bucket.name}/{key}')
        logger.info(f'Uploaded {total_rows} rows to stage')

    @staticmethod
    def get_now_timestamp() -> str:
        return datetime.datetime.now(ZoneInfo(default_timezone)).strftime(default_timestamp_format)

    def update_load_date_stmt(self, suffix: str = '') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')

        return f"""\
        UPDATE {self.table_uri}{suffix} SET load_date='{self.get_now_timestamp()}'""".replace(INDENT, '')

    def drop_staging_table_stmt(self) -> str:
        return f"""\
        DROP TABLE IF EXISTS {self.table_uri}{default_stage_suffix}""".replace(INDENT, '')

    def create_staging_table_stmt(self) -> str:
        return f"""\
        CREATE TABLE {self.table_uri}{default_stage_suffix} LIKE {self.table_uri}""".replace(INDENT, '')

    def load_stmt(self) -> dict[SQL, str]:
        raise NotImplementedError

    def load_stmt_str(self) -> str:
        return ';\n'.join(self.load_stmt().values())

    def get_latest_timestamp(self) -> str | None:
        if self.options.timestamp_field is None:
            raise ValueError('The timestamp_field in options is not set')
        response = get_extract_time_table().get_item(Key={'table_uri': self.table_uri})
        latest_timestamp = response.get('Item', {}).get(self.options.timestamp_field)
        return cast(str | None, latest_timestamp)

    def set_latest_timestamp(self, latest_timestamp: str):
        if self.options.timestamp_field is None:
            raise ValueError('The timestamp_field in options is not set')
        get_extract_time_table().put_item(
            Item={
                'table_uri': self.table_uri,
                self.options.timestamp_field: latest_timestamp,
                'updated': self.get_now_timestamp()
            }
        )
        logger.info(f'Set latest timestamp {self.options.timestamp_field}={latest_timestamp} for {self.table_uri}')

    def set_batches(self, rows_count: int):
        column_count = len(self.columns)
        batches = 1 + rows_count * column_count // 125_000_000
        self.options.batches = batches
        logger.info(f'Set {self.table_name}.options.batches={batches} [{column_count} columns, {rows_count} rows]')

    def load_from_stage(self) -> dict | None:
        """
        Load data from Snowflake stage into target table.
        """
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            logger.info(f'Connected to Snowflake account {snowflake_conn.account}.')
            with snowflake_conn.cursor() as snowflake_cursor:
                for cmd_type, stmt in self.load_stmt().items():
                    logger.info(stmt)
                    snowflake_cursor.execute(stmt)
                    results = [row for row in snowflake_cursor.fetchall()]
                    if results:
                        match cmd_type:
                            case SQL.drop | SQL.create | SQL.truncate:
                                logger.info(f'{cmd_type.value}: {results[0][0]}')
                            case SQL.update | SQL.insert | SQL.delete:
                                logger.info(f'{cmd_type.value}: {results[0][0]} rows affected.')
                            case SQL.copy:
                                for row in results:
                                    msg = 'file: {}, status: {}, parsed {}, loaded {}'.format(*row[:4])
                                    logger.info(f'{cmd_type.value}: {msg}')
                if self.options.timestamp_field:
                    snowflake_cursor.execute(f'SELECT MAX({self.options.timestamp_field}) FROM {self.table_uri}')
                    result: tuple = snowflake_cursor.fetchone()  # type: ignore
                    self.set_latest_timestamp(result[0])
        return None


class SnowflakeTableOverwrite(SnowflakeTable):
    def truncate_table_stmt(self) -> str:
        return f"""\
        TRUNCATE TABLE IF EXISTS {self.table_uri}""".replace(INDENT, '')

    def load_stmt(self) -> dict[SQL, str]:
        logger.info(f'Overwrite {self.table_uri}...')
        return {
            SQL.truncate: self.truncate_table_stmt(),
            SQL.copy: self.copy_table_stmt(),
            SQL.update: self.update_load_date_stmt(),
        }


class SnowflakeTableUpsert(SnowflakeTable):

    def delete_from_table_stmt(self) -> str:
        return f"""\
        DELETE FROM {self.table_uri} WHERE {self.options.primary_key} IN (SELECT {self.options.primary_key} FROM {self.table_uri}{default_stage_suffix})""".replace(
            INDENT, '')

    def insert_into_table_stmt(self) -> str:
        return f"""\
        INSERT INTO {self.table_uri} SELECT * FROM {self.table_uri}{default_stage_suffix}""".replace(INDENT, '')

    def load_stmt(self) -> dict[SQL, str]:
        logger.info(f'Upsert {self.table_uri} using primary key {self.options.primary_key}...')
        return {
            SQL.drop: self.drop_staging_table_stmt(),
            SQL.create: self.create_staging_table_stmt(),
            SQL.copy: self.copy_table_stmt(default_stage_suffix),
            SQL.update: self.update_load_date_stmt(default_stage_suffix),
            SQL.delete: self.delete_from_table_stmt(),
            SQL.insert: self.insert_into_table_stmt(),
        }


class SnowflakeSchema(ExtractLoadSchema[SnowflakeWarehouseType, SnowflakeSchemaType, SnowflakeTableType]):
    pass


class SnowflakeWarehouse(ExtractLoadWarehouse[SnowflakeWarehouseType, SnowflakeSchemaType, SnowflakeTableType]):
    schemas: dict[str, SnowflakeSchemaType] = field(default_factory=dict)

    def table_class(self, table_serialized: dict[str, dict | None]) -> type[SnowflakeTableType]:
        options = table_serialized.get('options') or {}
        has_primary_key = options.get('primary_key') is not None
        if has_primary_key:
            return cast(type[SnowflakeTableType], SnowflakeTableUpsert)
        return cast(type[SnowflakeTableType], SnowflakeTableOverwrite)

    def create_tables(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None,
            overwrite: bool = False,
            save_files: bool = False
    ):
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            with snowflake_conn.cursor() as snowflake_cursor:
                tables = self.filter(schema_names, table_names)
                for table in tables:
                    create_stmt = table.create_table_stmt()
                    if save_files:
                        path = Path('sql') / str(table.schema_name)
                        path.mkdir(exist_ok=True)
                        file = path / f'{table.table_name}.sql'
                        with file.open('w', encoding='utf-8') as f:
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

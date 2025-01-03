import csv
import datetime
import gzip
import io
import logging
import time
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from io import BytesIO
from io import StringIO
from itertools import batched
from pathlib import Path
from typing import Any
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
from blazel.tasks import ExtractLoadWarehouse
from blazel.tasks import TableTaskType

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


def size(len_bytes: int) -> str:
    if len_bytes < 1024:
        return f'{len_bytes} bytes'
    if len_bytes < 1024 ** 2:
        return f'{len_bytes / 1024:.2f} Kb'
    if len_bytes < 1024 ** 3:
        return f'{len_bytes / 1024 ** 2:.2f} Mb'
    return f'{len_bytes / 1024 ** 3:.2f} Gb'


@dataclass
class DataBytes:
    body: bytes
    file_number: int
    row_count: int

    @property
    def len(self):
        return len(self.body)

    @property
    def size(self) -> str:
        return size(self.len)


@dataclass
class CsvConfig:
    delimiter: str = ';'
    quotechar: str = '"'
    quoting: int = csv.QUOTE_MINIMAL
    escapechar: str = '\\'
    lineterminator: str = '\n'


csv_config = CsvConfig()


@dataclass
class GzipFileBuffer:
    max_file_size: int
    file: gzip.GzipFile = field(init=False)
    buffer: BytesIO = field(default_factory=BytesIO)
    file_number = 1
    row_count = 0

    def __post_init__(self):
        self.file = gzip.GzipFile(fileobj=self.buffer, mode='wb')

    @property
    def size(self) -> str:
        return size(self.buffer.getbuffer().nbytes)

    @property
    def is_too_large(self) -> bool:
        return self.buffer.getbuffer().nbytes >= self.max_file_size

    @staticmethod
    def get_csv_bytes(rows: tuple[tuple, ...]) -> bytes:
        csv_buffer = StringIO()
        csv_writer = csv.writer(
            csv_buffer,
            delimiter=csv_config.delimiter,
            quotechar=csv_config.quotechar,
            quoting=csv_config.quoting,
            escapechar=csv_config.escapechar,
            lineterminator=csv_config.lineterminator
        )
        csv_writer.writerows(rows)
        return csv_buffer.getvalue().encode()

    def write(self, rows: tuple[tuple, ...]):
        self.file.write(self.get_csv_bytes(rows))
        self.row_count += len(rows)

    def get_data_bytes(self):
        self.file.close()
        data_bytes = DataBytes(
            body=self.buffer.getvalue(),
            file_number=self.file_number,
            row_count=self.row_count,
        )
        self.buffer = BytesIO()
        self.file = gzip.GzipFile(fileobj=self.buffer, mode='wb')
        self.file_number += 1
        self.row_count = 0
        return data_bytes


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
        return {
            'message': f'Deleted {counter} file(s) from {path}'
        }

    def get_key(self, batch: int | str, file_number: int, suffix: str = 'csv.gz') -> str:
        batch = f'b{batch:02d}' if isinstance(batch, int) else batch
        file_name = f'{self.table_name}_{batch}_f{file_number:02d}.{suffix}'
        return f'{self.schema_name}/{self.table_name}/{file_name}'

    @staticmethod
    def get_data_bytes(data: Data, max_file_size: int, csv_batch_size: int) -> Generator[DataBytes, None, None]:
        fb = GzipFileBuffer(max_file_size)
        for rows in batched(data, csv_batch_size):
            row_count, column_count = len(rows), len(rows[0])
            start_time = time.time()
            fb.write(rows)
            logger.info(
                f'Writing {row_count} rows [{row_count * column_count} entries] '
                f'finished in {time.time() - start_time:.2f} seconds. '
                f'File buffer [{fb.size}] contains {fb.row_count} rows.'
            )
            if fb.is_too_large:
                logger.info(f'File buffer exceeds maximum size ({size(max_file_size)}). Yield data bytes ({fb.size}).')
                yield fb.get_data_bytes()
        if fb.row_count > 0:
            logger.info(f'Yield final data bytes ({fb.size}).')
            yield fb.get_data_bytes()

    def download_from_stage(
            self,
            batch_number: int = 0,
            file_number: int = 0,
            raw: bool = False
    ) -> list[str] | list[list[Any]]:
        bucket = get_snowflake_staging_bucket()
        key = self.get_key(batch_number, file_number)
        obj = bucket.Object(key)
        logger.info(f'Downloading s3://{bucket.name}/{key}')
        csv_str = gzip.decompress(obj.get()['Body'].read()).decode()
        data: list[str] | list[list[Any]]
        if raw:
            data = csv_str.splitlines()
        else:
            reader = csv.reader(
                io.StringIO(csv_str),
                delimiter=csv_config.delimiter,
                quotechar=csv_config.quotechar,
                quoting=csv_config.quoting,
                escapechar=csv_config.escapechar,
                lineterminator=csv_config.lineterminator
            )
            data = [row for row in reader]
        return data

    def upload_to_stage(
            self,
            data: Data,
            batch_number: int = 0,
            max_file_size: int = 15 * 1024 * 1024,  # 15 Mb
            csv_batch_size: int = 25_000,
            total_rows: int = 0
    ) -> dict | None:
        bucket = get_snowflake_staging_bucket()
        row_count, files, files_len = 0, 0, 0
        for data_bytes in self.get_data_bytes(data, max_file_size, csv_batch_size):
            row_count += data_bytes.row_count
            files = data_bytes.file_number
            files_len += data_bytes.len
            key = self.get_key(batch_number, data_bytes.file_number)
            bucket.put_object(Body=data_bytes.body, Key=key)
            logger.info(f'Uploaded {data_bytes.size} [{data_bytes.row_count} rows] to s3://{bucket.name}/{key}')
            if total_rows:
                logger.info(
                    f'Processed {row_count / total_rows:.2%} of rows '
                    f'using {self.relative_time():.2%} of available time.'
                )
        message = (
            f'Task [{batch_number}] uploaded {size(files_len)} [{files} file(s), {row_count} rows] to s3://{bucket.name} '
            f'using {self.relative_time():.2%} of available time.'
        )
        logger.info(message)
        return {'message': message}

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.table_uri}{suffix} ({column_names})
        FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        FILE_FORMAT = ( TYPE = CSV FIELD_DELIMITER = ';' EMPTY_FIELD_AS_NULL = TRUE SKIP_BLANK_LINES = TRUE TRIM_SPACE = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"' )""".replace(
            INDENT, '')

    @staticmethod
    def get_now_timestamp() -> str:
        return datetime.datetime.now(ZoneInfo(default_timezone)).strftime(default_timestamp_format)

    def truncate_table_stmt(self) -> str:
        return f"""\
        TRUNCATE TABLE IF EXISTS {self.table_uri}""".replace(INDENT, '')

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
        logger.info(f'Overwrite {self.table_uri}...')
        return {
            SQL.truncate: self.truncate_table_stmt(),
            SQL.copy: self.copy_table_stmt(),
            SQL.update: self.update_load_date_stmt(),
        }

    def load_stmt_str(self) -> str:
        return ';\n'.join(self.load_stmt().values())

    def get_latest_timestamp(self) -> str | None:
        if self.options.timestamp_field is None:
            raise ValueError('The timestamp_field in options is not set')
        response = get_extract_time_table().get_item(Key={'table_uri': self.table_uri})
        latest_timestamp = response.get('Item', {}).get(self.options.timestamp_field)
        return cast(str | None, latest_timestamp)

    def set_latest_timestamp(self, latest_timestamp: str | datetime.datetime | None):
        if self.options.timestamp_field is None:
            raise ValueError('The timestamp_field in options is not set')
        if isinstance(latest_timestamp, (datetime.datetime, datetime.date)):
            latest_timestamp = latest_timestamp.strftime(default_timestamp_format)
        get_extract_time_table().put_item(
            Item={
                'table_uri': self.table_uri,
                self.options.timestamp_field: latest_timestamp,
                'updated': self.get_now_timestamp()
            }
        )
        logger.info(f'Set latest timestamp {self.options.timestamp_field}={latest_timestamp} for {self.table_uri}')

    def load_from_stage(self) -> dict | None:
        """
        Load data from Snowflake stage into target table.
        """
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            logger.info(f'Connected to Snowflake account {snowflake_conn.account}.')
            messages = []
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
                                    if len(row) == 1:
                                        msg = row[0]  # error message
                                    else:
                                        msg = 'file: {}, status: {}, parsed {}, loaded {}'.format(*row[:4])
                                    messages.append(f'{cmd_type.value}: {msg}')
                                logger.info('\n'.join(messages))
                if self.options.timestamp_field:
                    snowflake_cursor.execute(f'SELECT MAX({self.options.timestamp_field}) FROM {self.table_uri}')
                    result: tuple = snowflake_cursor.fetchone()  # type: ignore
                    self.set_latest_timestamp(result[0])
        return {
            'message': messages
        }

    def update_timestamp_field(self):
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            with snowflake_conn.cursor() as snowflake_cursor:
                if self.options.timestamp_field:
                    snowflake_cursor.execute(f'SELECT MAX({self.options.timestamp_field}) FROM {self.table_uri}')
                    result: tuple = snowflake_cursor.fetchone()  # type: ignore
                    self.set_latest_timestamp(result[0])


class SnowflakeTableOverwrite(SnowflakeTable):
    pass


class SnowflakeTableUpsert(SnowflakeTable):

    def delete_from_table_stmt(self) -> str:
        return f"""\
        DELETE FROM {self.table_uri} WHERE {self.options.primary_key} IN (SELECT {self.options.primary_key} FROM {self.table_uri}{default_stage_suffix})""".replace(
            INDENT, '')

    def insert_into_table_stmt(self) -> str:
        return f"""\
        INSERT INTO {self.table_uri} SELECT * FROM {self.table_uri}{default_stage_suffix}""".replace(INDENT, '')

    def load_stmt(self) -> dict[SQL, str]:
        if self.options.truncate:
            logger.info(f'Explicit truncate in options for {self.table_uri}.')
            return super(self).load_stmt()

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

import csv
import datetime
import gzip
import itertools
import json
import logging
import os
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from enum import Enum
from io import BytesIO
from io import StringIO
from itertools import batched
from pathlib import Path
from typing import Any
from typing import cast
from typing import Generator
from typing import Iterable
from typing import NamedTuple
from typing import TypeVar
from zoneinfo import ZoneInfo

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import connect
from snowflake.connector import DictCursor
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

from blazel.base import Column
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

SnowflakeSchema = ExtractLoadSchema[SnowflakeWarehouseType, SnowflakeSchemaType, SnowflakeTableType]

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


class CsvConfig(NamedTuple):
    delimiter: str
    quotechar: str
    quoting: int
    escapechar: str
    lineterminator: str


default_csv_config = CsvConfig(
    delimiter=';',
    quotechar='"',
    quoting=csv.QUOTE_MINIMAL,
    escapechar='\\',
    lineterminator='\n'
)


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
        csv_writer = csv.writer(csv_buffer, **default_csv_config._asdict())
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


class SnowflakeTable(
    ExtractLoadTable[
        SnowflakeSchemaType,
        SnowflakeTableType,
        TableTaskType
    ]
):

    def create_table_stmt(self) -> str:
        def f_comment(comment: str | None) -> str:
            return f" COMMENT '{comment}'" if comment else ''

        indent = 4 * ' '
        columns = ',\n'.join(
            [f'{indent}{column.name} {column.dtype.upper()}{f_comment(column.description)}'
             for column in self] + [f'{indent}load_date DATETIME']
        )
        return f"DROP TABLE IF EXISTS {self.table_uri};\n" \
               f"CREATE TABLE {self.table_uri} (\n{columns}\n)"

    def clean_stage(self) -> dict | None:
        prefix = f'{self.schema.name}/{self.name}/'
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

    def get_key(self, batch: int | str, file: int | str, suffix: str = 'csv.gz') -> str:
        batch = f'b{batch:02d}' if isinstance(batch, int) else batch
        file = f'f{file:02d}' if isinstance(file, int) else file
        file_name = f'{self.name}_{batch}_{file}.{suffix}'
        return f'{self.schema.name}/{self.name}/{file_name}'

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
            batch: int | str = 0,
            file: int | str = 0
    ) -> str:
        bucket = get_snowflake_staging_bucket()
        key = self.get_key(batch, file)
        obj = bucket.Object(key)
        logger.info(f'Downloading s3://{bucket.name}/{key}')
        csv_str = gzip.decompress(obj.get()['Body'].read()).decode()
        return csv_str

    def convert_to_data(self, rows_dicts: Iterable[dict[str, Any]]) -> Generator[tuple[Any, ...], None, None]:
        column_names = [column.source if column.source else column.name for column in self]
        for row in rows_dicts:
            yield tuple(row.get(column_name) for column_name in column_names)
        return

    def upload_to_stage(
            self,
            data: Data,
            batch: int | str = 0,
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
            key = self.get_key(batch, data_bytes.file_number)
            bucket.put_object(Body=data_bytes.body, Key=key)
            logger.info(f'Uploaded {data_bytes.size} [{data_bytes.row_count} rows] to s3://{bucket.name}/{key}')
            if total_rows:
                logger.info(
                    f'Processed {row_count / total_rows:.2%} of rows '
                    f'using {self.relative_time():.2%} of available time.'
                )
        message = (
            f'Task [{batch}] uploaded {size(files_len)} [{files} file(s), {row_count} rows] to s3://{bucket.name} '
            f'using {self.relative_time():.2%} of available time.'
        )
        logger.info(message)
        return {'message': message}

    def file_format(self) -> str:
        if self.meta.stage_file_format:
            return f"FORMAT_NAME = '{self.database_name}.public.{self.meta.stage_file_format}'"
        match self.meta.file_format:
            case 'csv':
                return """
                TYPE = CSV
                FIELD_DELIMITER = ';'
                SKIP_BLANK_LINES = TRUE
                TRIM_SPACE = TRUE
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                """.replace(INDENT, '')
            case 'parquet':
                return "TYPE = PARQUET"
            case _:
                raise ValueError(f"Unsupported file_format '{self.meta.file_format}'")

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        stage = os.environ.get('DATABASE_STAGE', 'public.stage')

        stage_files = f'{self.database_name}.{stage}/{self.schema.name}/{self.name}/'
        match self.meta.file_format:
            case 'csv':
                copy_table_stmt = f"""\
                COPY INTO {self.table_uri}{suffix} ({column_names})
                FROM @{stage_files}
                FILE_FORMAT = ( {self.file_format()} )""".replace(INDENT, '')
            case 'parquet':
                def format_timestamp(column: Column) -> str:
                    if column.dtype == 'timestamp':
                        return f'TO_TIMESTAMP_NTZ($1:{column.name}::int, 6)'
                    else:
                        return f'$1:{column.name}::{column.dtype}'

                conv_columns_str = ',\n'.join(format_timestamp(column) for column in self)
                copy_table_stmt = f"""\
                COPY INTO {self.table_uri}{suffix} ({column_names}) FROM (
                    SELECT {conv_columns_str}
                    FROM @{stage_files}
                )
                FILE_FORMAT = ( {self.file_format()} )""".replace(INDENT, '')
            case _:
                raise ValueError(f"Unsupported file_format '{self.meta.file_format}'")
        return copy_table_stmt

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
        if self.meta.timestamp_field is None:
            raise ValueError('The timestamp_field in meta is not set')
        response = get_extract_time_table().get_item(Key={'table_uri': self.table_uri})
        latest_timestamp = response.get('Item', {}).get(self.meta.timestamp_field)
        return cast(str | None, latest_timestamp)

    def set_latest_timestamp(self, latest_timestamp: str | datetime.datetime | None):
        if self.meta.timestamp_field is None:
            raise ValueError('The timestamp_field in meta is not set')
        if isinstance(latest_timestamp, (datetime.datetime, datetime.date)):
            latest_timestamp = latest_timestamp.strftime(default_timestamp_format)
        get_extract_time_table().put_item(
            Item={
                'table_uri': self.table_uri,
                self.meta.timestamp_field: latest_timestamp,
                'updated': self.get_now_timestamp()
            }
        )
        logger.info(f'Set latest timestamp {self.meta.timestamp_field}={latest_timestamp} for {self.table_uri}')

    def load_from_stage(self) -> dict | None:
        """
        Load data from Snowflake stage into target table.
        """
        messages = []
        with self.schema.warehouse.cursor() as cursor:
            for cmd_type, stmt in self.load_stmt().items():
                logger.info(stmt)
                cursor.execute(stmt)
                results = [row for row in cursor.fetchall()]
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
            if self.meta.timestamp_field:
                cursor.execute(f'SELECT MAX({self.meta.timestamp_field}) FROM {self.table_uri}')
                result: tuple = cursor.fetchone()  # type: ignore
                self.set_latest_timestamp(result[0])
        return {
            'message': messages if len(messages) <= 3 else f'Loaded {len(messages)} files into {self.table_uri}.'
        }

    def update_timestamp_field(self, value: str | datetime.datetime | None = None):
        """
        Update the timestamp field in the extract_time_table.

        Args:
            value:

        Returns:

        """
        if isinstance(value, datetime.datetime):
            value = value.strftime(default_timestamp_format)
        with self.schema.warehouse.cursor() as cursor:
            if self.meta.timestamp_field:
                if value is None:
                    cursor.execute(f'SELECT MAX({self.meta.timestamp_field}) FROM {self.table_uri}')
                    result: tuple = snowflake_cursor.fetchone()  # type: ignore
                    value = result[0]
                self.set_latest_timestamp(value)

    def get_stats(self, no_cache=False) -> dict:
        file = Path(f'data/.cache/{self.schema.name}/{self.name}.json')
        if file.exists() and not no_cache:
            logger.info(f'Loading stats from {file}')
            stats = json.load(file.open())
            return stats

        logger.info(f'Loading stats from {self.table_uri} ({len(self.columns)} columns)...')
        with self.schema.warehouse.cursor(DictCursor) as cursor:
            n = cursor.execute(f'SELECT COUNT(*) AS "n" FROM {self.table_uri}').fetchone()['n']
            result = {}
            for batch in itertools.batched(self, n=20):
                fields = []
                column: Column
                for column in batch:
                    for metric, formula in {
                        'min': 'MIN({column})',
                        'max': 'MAX({column})',
                        'count': 'COUNT({column})',
                        'count distinct': 'COUNT(DISTINCT {column})',
                        'sample': '(SELECT ARRAY_AGG({column}) FROM (SELECT DISTINCT {column} FROM ' + self.table_uri + ' SAMPLE (10 ROWS)))'
                    }.items():
                        fields.append({
                            'column': column.name.strip('"'),
                            'metric': metric,
                            'formula': formula.format(column=column.name)
                        })
                fields_str = ','.join('{formula} AS "{column}|{metric}"'.format(**s) for s in fields)
                select = f'SELECT {fields_str} FROM {self.table_uri}'
                logger.debug(select)
                cursor.execute(select)
                result |= cursor.fetchone()
        stats = defaultdict(dict)
        for column_metric, value in result.items():
            column, metric = column_metric.split('|')
            stats[column][metric] = value
        for item in stats.values():
            if isinstance(item['min'], Decimal):
                item['min'] = float(item['min'])
                item['max'] = float(item['max'])
            item['not null'] = item['count'] == n
            item['unique'] = item['count distinct'] == n
            item['sample'] = json.loads(item['sample'])
        file.parent.mkdir(parents=True, exist_ok=True)
        json_str = json.dumps(stats, indent=2, ensure_ascii=False, default=str)
        file.write_text(json_str)
        return stats

    def get_auto_doc(self) -> str:
        from blazel.handler.aiagent import Claude
        args = {
            'table_name': self.name,
            'description': self.description,
            'primary_key': self.meta.primary_key,
        }
        stats = self.get_stats()
        for column in self:
            name = column.name.strip('"')
            stats[name]['data_type'] = column.dtype
            if column.description:
                stats[name]['description'] = column.description
        args['columns'] = stats
        example = """\
        {
          "description": "<table description>"
          "columns": {
              "<column name>": "<column description>"
          }
        }
        """.replace(INDENT, '')
        prompt = f"""\
        You create structured documentation for our data warehouse. 
        You analyze tables in the schema `{self.schema.name}`. For context on the schema:
        {self.schema.description}
         
        You analyze the table `{self.name}` and its {len(self.columns)} columns using the information and statistics from the following JSON:
        
        ```
        {json.dumps(stats, indent=2, ensure_ascii=False, default=str)}
        ```
        
        Understand or guess the purpose of each single columns.
        Write a short description for each column and the entire table.
        Return your answer in JSON following this structure:
        ```
        {example}
        ```
        Only return the valid JSON. No introduction. No explanation.        
        """.replace(INDENT, '')
        logger.debug(prompt)
        model = Claude()
        logger.info(f'Calling {model.model_id}...')
        answer = model.invoke(prompt)
        print(answer)
        result = json.loads(answer)
        return result


class SnowflakeTableUpsert(SnowflakeTable):

    def delete_by_primary_key(self) -> str:
        if self.meta.primary_key is None:
            raise ValueError('Primary key is not set')
        primary_key_columns = self.meta.primary_key.split(';')
        where_clause = ' AND '.join(
            f'{self.name}.{column} = {self.name}{default_stage_suffix}.{column}'
            for column in primary_key_columns
        )
        return f"""\
        DELETE FROM {self.table_uri}
        USING {self.table_uri}{default_stage_suffix}
        WHERE {where_clause}""".replace(INDENT, '')

    def delete_by_datetime_range(self) -> str:
        return f"""\
        DELETE FROM {self.table_uri}
        USING (
            SELECT
                MIN({self.meta.timestamp_key}) AS min_ts,
                MAX({self.meta.timestamp_key}) AS max_ts
            FROM {self.table_uri}{default_stage_suffix}
        ) AS range
        WHERE ({self.meta.timestamp_key} BETWEEN range.min_ts AND range.max_ts)
            OR {self.meta.timestamp_key} IS NULL""".replace(INDENT, '')

    def delete_from_table_stmt(self) -> str:
        if self.meta.primary_key:
            return self.delete_by_primary_key()
        if self.meta.timestamp_key:
            return self.delete_by_datetime_range()
        raise ValueError('Primary key or timestamp_key is required for upsert.')

    def insert_into_table_stmt(self) -> str:
        return f"""\
        INSERT INTO {self.table_uri} SELECT * FROM {self.table_uri}{default_stage_suffix}""".replace(INDENT, '')

    def load_stmt(self) -> dict[SQL, str]:
        if self.meta.truncate:
            logger.info(f'Explicit truncate in meta for {self.table_uri}.')
            return super().load_stmt()

        logger.info(f'Upsert {self.table_uri} using primary key {self.meta.primary_key}...')
        return {
            SQL.drop: self.drop_staging_table_stmt(),
            SQL.create: self.create_staging_table_stmt(),
            SQL.copy: self.copy_table_stmt(default_stage_suffix),
            SQL.update: self.update_load_date_stmt(default_stage_suffix),
            SQL.delete: self.delete_from_table_stmt(),
            SQL.insert: self.insert_into_table_stmt(),
        }


class SnowflakeWarehouse(
    ExtractLoadWarehouse[
        SnowflakeWarehouseType,
        SnowflakeSchemaType,
        SnowflakeTableType
    ]
):
    schemas: dict[str, SnowflakeSchemaType] = field(default_factory=dict)

    @contextmanager
    def cursor(self, cursor_class: type[SnowflakeCursor] = SnowflakeCursor):
        with get_snowflake_connection(self.name) as snowflake_conn:
            logger.info(f'Connected to Snowflake account {snowflake_conn.account}.')
            with snowflake_conn.cursor(cursor_class) as snowflake_cursor:
                yield snowflake_cursor

    @staticmethod
    def table_class(table_serialized: dict[str, dict | None]) -> type[SnowflakeTableType]:
        options = table_serialized.get('meta') or {}
        has_upsert_key = options.get('primary_key') is not None or options.get('timestamp_key') is not None
        if has_upsert_key:
            return cast(type[SnowflakeTableType], SnowflakeTableUpsert)
        return cast(type[SnowflakeTableType], SnowflakeTable)

    def create_tables(
            self,
            schema_names: set[str] | list[str] | None = None,
            table_names: set[str] | list[str] | None = None,
            overwrite: bool = False,
            save_files: bool = False
    ):
        with self.cursor() as cursor:
            schemas = self.filter_schemas(schema_names)
            for schema in schemas:
                schema_uri = f'{self.name}.{schema.name}'
                if table_names is None and overwrite:
                    logger.info(f'Dropped {schema_uri}.')
                    cursor.execute(f'DROP SCHEMA IF EXISTS {schema_uri} CASCADE')
                    overwrite = False
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_uri}')
                for table in schema.filter_tables(table_names):
                    create_stmt = table.create_table_stmt()
                    if save_files:
                        path = Path('sql') / str(table.schema.name)
                        path.mkdir(exist_ok=True)
                        file = path / f'{table.name}.sql'
                        with file.open('w', encoding='utf-8') as f:
                            f.write(create_stmt)
                    drop_stmt, create_stmt = create_stmt.split(';')
                    if overwrite:
                        cursor.execute(drop_stmt)
                        logger.info(f'Dropped {table.table_uri}.')
                    try:
                        cursor.execute(create_stmt)
                        logger.info(f'Created {table.table_uri}.')
                    except ProgrammingError as e:
                        if e.errno == 2002:
                            logger.info(f'Table {table.table_uri} exists. Skipping.')
                        else:
                            raise

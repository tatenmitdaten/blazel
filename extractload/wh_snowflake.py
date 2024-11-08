import csv
import datetime
import gzip
import io
import logging
from dataclasses import dataclass
from itertools import batched
from pathlib import Path
from zoneinfo import ZoneInfo

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError

from extractload.clients import get_parameters
from extractload.clients import get_secret
from extractload.clients import get_snowflake_staging_bucket
from extractload.config import default_timestamp_format
from extractload.config import default_timezone
from extractload.wh_base import DbWarehouse
from extractload.wh_base import TableType
from extractload.tasks import Data
from extractload.tasks import ExtractTable

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
    snowflake_secret_arn = get_parameters()['SnowflakeSecretArn']
    secret: dict = get_secret(snowflake_secret_arn)
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
        if table_serialized.get('options', {}).get('primary_key') is None:
            return SnowflakeUpdateTable
        return SnowflakeUpsertTable

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
    @property
    def load_stmt(self) -> str:
        raise NotImplementedError

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

    def upload_to_stage(self, data: Data, chunk_size: int = 500_000):
        total_rows = 0
        bucket = get_snowflake_staging_bucket()
        for file_number, rows in enumerate(batched(data, chunk_size)):
            total_rows += len(rows)
            file_name = f'file_{file_number:02d}'
            key = f'{self.schema_name}/{self.table_name}/{file_name}.csv.gz'
            body = gzip.compress(self.rows_dict_to_csv(rows).encode('utf-8'))
            bucket.put_object(Body=body, Key=key)
            logger.info(f'Uploaded {len(body)} bytes to s3://{bucket.name}/{key}')
        logger.info(f'Uploaded {total_rows} rows to stage')

    def rows_dict_to_csv(self, rows_dict: tuple[dict[str, object], ...]) -> str:
        fieldnames = [column.name.strip('"') for column in self]
        utc_extract_ts = datetime.datetime.now(datetime.UTC).strftime(default_timestamp_format)
        with io.StringIO() as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                delimiter=';',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator='\n',
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in rows_dict:
                if 'utc_extract_ts' in self.columns:
                    row['utc_extract_ts'] = utc_extract_ts
                try:
                    writer.writerow(row)
                except ValueError:
                    print(row)
                    raise
            return csv_file.getvalue()

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

    def copy_table_stmt(self, suffix='') -> str:
        if suffix not in ('', default_stage_suffix):
            raise ValueError(f'Invalid suffix: {suffix}')
        column_names = ', '.join(column.name for column in self)
        return f"""\
        COPY INTO {self.table_uri}{suffix} ({column_names})
        FROM @{self.database_name}.public.stage/{self.schema_name}/{self.table_name}/
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ';'
            EMPTY_FIELD_AS_NULL = TRUE
            SKIP_HEADER = 1
            SKIP_BLANK_LINES = TRUE
            TRIM_SPACE = TRUE,
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )"""

    def load_from_stage(self):
        """
        Load warehouse_serialized from Snowflake stage into target table.
        """
        with get_snowflake_connection(self.database_name) as snowflake_conn:
            logger.info(f'Connected to Snowflake account {snowflake_conn.account}.')
            with snowflake_conn.cursor() as snowflake_cursor:
                for stmt in self.load_stmt.split(';'):
                    if stmt.strip():
                        logger.info(stmt.replace(8 * ' ', ''))
                        snowflake_cursor.execute(stmt)
                results = [row[0] for row in snowflake_cursor.fetchall()]
                logger.info(f'Loaded {results} rows into "{self.table_uri}" in parallel')


@dataclass
class SnowflakeUpdateTable(SnowflakeTable):

    def truncate_table_stmt(self) -> str:
        return f"""\
        TRUNCATE TABLE IF EXISTS {self.table_uri}
        """

    @property
    def load_stmt(self) -> str:
        logger.info(f'Replace warehouse_serialized in {self.table_uri}...')
        return ';\n'.join([
            self.truncate_table_stmt(),
            self.copy_table_stmt(),
            self.update_load_date_stmt(),
        ]).replace(8 * ' ', ' ')


@dataclass
class SnowflakeUpsertTable(SnowflakeTable):

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

    @property
    def load_stmt(self) -> str:
        logger.info(f'Merging into {self.table_uri} using primary key {self.options.primary_key}...')
        return ';\n'.join([
            self.staging_table_stmt(),
            self.copy_table_stmt(default_stage_suffix),
            self.update_load_date_stmt(default_stage_suffix),
            self.delete_append_table_stmt(),
        ]).replace(8 * ' ', ' ')

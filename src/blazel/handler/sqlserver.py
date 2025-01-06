import copy
import logging
import re
from contextlib import contextmanager

import pyodbc  # type: ignore

from blazel.base import BaseWarehouse
from blazel.base import Column
from blazel.handler.database import BaseDatabase

logger = logging.getLogger()


class SQLServerDatabase(BaseDatabase['SQLServerDatabase']):
    odbc_driver_path = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1'

    @contextmanager
    def get_conn(self, login_timeout: int = 5, conn_timeout: int = 240) -> pyodbc.Connection:
        conn_string = f'driver={self.odbc_driver_path};uid={self.username};pwd={self.password};database={self.dbname}'
        timeouts = f'[login timeout: {login_timeout}s, conn timeout: {conn_timeout}s]'
        if self.use_tunnel:
            conn_string = f'{conn_string};server={self.host},{self.port}'
            logger.info(f'Connecting to {self.host}:{self.port} {timeouts}')
            with pyodbc.connect(conn_string, timeout=login_timeout) as conn:
                conn.timeout = conn_timeout
                yield conn
        else:
            with self.get_ssh_tunnel() as tunnel:
                server = self._tunnel_server()
                port = tunnel.local_bind_address[1]
                conn_string = f'{conn_string};server={server},{port}'
                logger.info(f'Connecting to {self.host} via {server}:{port} {timeouts}')
                with pyodbc.connect(conn_string, timeout=login_timeout) as conn:
                    conn.timeout = conn_timeout
                    yield conn


def _adapt_column_name(column_name: str) -> str:
    replacements = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue',
        '&': '_and'
    }
    for old, new in replacements.items():
        column_name = column_name.replace(old, new)
    parts: list[str] = column_name.split(' ')
    for i, part in enumerate(parts):
        if part.isupper():
            parts[i] = part.lower()
        else:
            parts[i] = re.sub(r'(?<!^)(?<![A-Z])(?=[A-Z])', '_', part).lower()
    return '_'.join(parts)


def _create_columns(description: tuple) -> dict[str, Column]:
    columns = {}
    for mssql_column in description:
        name = _adapt_column_name(mssql_column[0])
        if mssql_column[1] == str:
            dtype = 'VARCHAR'
        else:
            dtype = str(mssql_column[1].__name__).upper()
            if dtype == 'DECIMAL':
                precision = mssql_column[4]
                scale = mssql_column[5]
                dtype = f'DECIMAL({precision},{scale})'
        column = Column(
            name=name,
            dtype=dtype.lower(),
            definition=f'"{mssql_column[0]}"'
        )
        columns[name] = column
    return columns


def inspect_tables(
        database: SQLServerDatabase,
        warehouse: BaseWarehouse,
        schema_names: list[str]
) -> BaseWarehouse:
    warehouse = copy.deepcopy(warehouse)
    # noinspection PyArgumentList
    with database.get_conn() as conn:
        with conn.cursor() as cursor:
            for table in warehouse.filter(schema_names=schema_names):
                source_name = table.options.source_name
                logger.info(source_name)
                cursor.execute(f"SELECT * FROM {source_name} WHERE 1=0")
                table.columns = _create_columns(cursor.description)
    return warehouse

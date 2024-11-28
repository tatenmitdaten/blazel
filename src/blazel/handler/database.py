import io
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import paramiko.rsakey
import sshtunnel  # type: ignore

from blazel.clients import get_secretsmanager_client
from blazel.tasks import Data

logger = logging.getLogger()


@dataclass
class BaseDatabase:
    username: str
    password: str
    host: str | None = None
    port: int | None = None
    dbname: str | None = None
    engine: str | None = None

    def __post_init__(self):
        if self.port is not None:
            self.port = int(self.port)

    @property
    def ssh_tunnel_host_ip(self):
        try:
            return os.environ['SSH_HOST_IP']
        except KeyError:
            raise ValueError('SSH_HOST_IP environment variable is not set.')

    @property
    def ssh_host_private_key_secret_id(self):
        try:
            return os.environ['SSH_HOST_PRIVATE_KEY_SECRET_ID']
        except KeyError:
            raise ValueError('SSH_HOST_PRIVATE_KEY_SECRET_ID environment variable is not set.')

    @property
    def use_tunnel(self) -> bool:
        return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None

    @classmethod
    @lru_cache
    def from_secret(cls, secret_id: str) -> 'BaseDatabase':
        client = get_secretsmanager_client()
        secret_str = client.get_secret_value(SecretId=secret_id)['SecretString']
        secret = json.loads(secret_str)
        return cls(**secret)

    @contextmanager
    def get_conn(self, login_timeout: int = 5, conn_timeout: int = 240):
        pass

    @contextmanager
    def get_ssh_tunnel(self):
        client = get_secretsmanager_client()
        secret = client.get_secret_value(SecretId=self.ssh_host_private_key_secret_id)['SecretString']
        jumphost_private_key = paramiko.rsakey.RSAKey.from_private_key(io.StringIO(secret))

        with sshtunnel.SSHTunnelForwarder(
                ssh_address_or_host=(self.ssh_tunnel_host_ip, 22),
                ssh_username='ubuntu',
                remote_bind_address=(self.host, self.port),
                ssh_pkey=jumphost_private_key
        ) as tunnel:
            yield tunnel

    def _tunnel_server(self) -> str:
        server = '127.0.0.1'
        file = Path('/etc/hosts')
        if file.exists():
            with file.open() as f:
                lines = f.readlines()
            hosts = {
                re.split(r'[ \t]+', line)[1].strip()
                for line in lines
                if line.startswith(server)
            }
            if self.host in hosts:
                server = self.host
        return server


def batch_cursor(cursor, batch_size) -> Data:
    while True:
        start_time = time.time()
        batch = cursor.fetchmany(batch_size)
        if not batch:
            logger.info('No more rows to fetch.')
            break
        rows, entries = len(batch), len(batch) * len(batch[0])
        logger.info(f'Fetched {rows} rows [{entries} entries] in {time.time() - start_time:.2f} seconds.')
        for row in batch:
            yield row
    return []

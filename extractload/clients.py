import importlib.resources
import json
import os
from enum import Enum
from functools import lru_cache
from typing import Generator

import boto3
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_secretsmanager import SecretsManagerClient
from mypy_boto3_stepfunctions import SFNClient
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect

DictRow = dict[str, object]
Data = Generator[DictRow, None, None] | list[DictRow]

class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'


@lru_cache
def get_parameters(env_: str = 'dev', filename: str = 'samconfig.yaml'):
    package_dir = importlib.resources.files('extractload')
    with (package_dir / '..' / filename).open('r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    params_raw = config[env_]['deploy']['parameters']['parameter_overrides']
    return dict(p.split('=', 1) for p in params_raw)


def get_env() -> Env:
    env_str = os.getenv('APP_ENV', 'dev')
    try:
        return Env(env_str)
    except ValueError:
        raise ValueError(f"Invalid environment '{env_str}'. Must be one of: {[e.value for e in Env]}")


def get_s3_client() -> S3Client:
    return boto3.client('s3')


def get_stepfunctions_client() -> SFNClient:
    return boto3.client('stepfunctions')


def get_secretsmanager_client() -> SecretsManagerClient:
    return boto3.client('secretsmanager')


def get_snowflake_staging_bucket() -> Bucket:
    """
    Get Snowflake staging bucket.

    Returns:
        Bucket: S3 bucket.
    """

    snowflake_staging_bucket_stem = get_parameters()['SnowflakeStagingBucketStem']
    bucket_name = f'{snowflake_staging_bucket_stem}-{get_env().value}'
    return boto3.resource('s3').Bucket(bucket_name)


@lru_cache
def get_secret(secret_id: str) -> dict:
    """
    Get secret from Secrets Manager and cache it.

    Returns:
        dict[str, dict[str, str]]: Credentials.
    """
    response = get_secretsmanager_client().get_secret_value(SecretId=secret_id)
    secret_string = response['SecretString']
    return json.loads(secret_string)


@lru_cache
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

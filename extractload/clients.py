import json
from functools import lru_cache

import boto3
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_secretsmanager import SecretsManagerClient
from mypy_boto3_stepfunctions import SFNClient

from config import Env
from config import get_parameters


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
    bucket_name = f'{snowflake_staging_bucket_stem}-{Env.get().value}'
    return boto3.resource('s3').Bucket(bucket_name)


@lru_cache
def get_secret(secret_id: str) -> dict[str, dict[str, str]]:
    """
    Get secret from Secrets Manager and cache it.

    Returns:
        dict[str, dict[str, str]]: Credentials.
    """
    response = get_secretsmanager_client().get_secret_value(SecretId=secret_id)
    secret_string = response['SecretString']
    return json.loads(secret_string)

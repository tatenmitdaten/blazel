import logging
import os
from enum import Enum
from functools import lru_cache
from pathlib import Path

import yaml

logger = logging.getLogger()

default_timestamp_format = '%Y-%m-%dT%H:%M:%S'
default_timezone = 'Europe/Berlin'


class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'

    @staticmethod
    def get() -> 'Env':
        return Env(os.getenv('APP_ENV', 'dev'))

    @staticmethod
    def set(env: str):
        os.environ['APP_ENV'] = Env(env).value

    @staticmethod
    def is_prod() -> bool:
        return Env.get() == Env.prod


@lru_cache
def get_parameters(file: Path | None = None) -> dict:
    """
    Get parameters from SAM config file
    Args:
        file: optional Path to SAM config file

    Returns:
        dict: Parameters
    """
    if file is None:
        if 'SAM_CONFIG_FILE' in os.environ:
            file = Path(os.environ['SAM_CONFIG_FILE'])
        else:
            file = Path('/var/task/samconfig.yaml')
        logger.info(f'Using sam config file "{file.absolute()}"')
    if not file.exists():
        raise FileNotFoundError(f'File "{file.absolute()}" not found. Cannot read parameters.')
    with file.open() as buffer:
        config = yaml.safe_load(buffer)
    env = Env.get().value
    params = config[env]['deploy']['parameters']
    overrides = params['parameter_overrides']
    return {
        'profile': params['profile'],
        **dict(p.split('=', 1) for p in overrides)
    }

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
        env = os.getenv('APP_ENV', 'dev')
        try:
            return Env(env)
        except ValueError:
            raise ValueError(f"Invalid environment '{env}'. Must be one of: {[str(e) for e in Env]}")


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
            file = Path('/var/task/extractload/samconfig.yaml')
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

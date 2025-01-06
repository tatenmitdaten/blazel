import os

import pytest
from moto import mock_aws


@pytest.fixture(scope='session')
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope='session')
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"


@pytest.fixture(scope='session')
def mocked_aws(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture(scope='session')
def parameters(monkeysession):
    parameter_dict = {
        'JobTableStem': 'job-table',
        'TaskTableStem': 'task-table',
        'ExtractTimeTableStem': 'extract-time-table',
        'SnowflakeStagingBucketStem': 'snowflake-staging-bucket',
    }
    monkeysession.setattr('blazel.clients.get_parameters', lambda: parameter_dict)
    return parameter_dict

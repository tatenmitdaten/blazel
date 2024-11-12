import json
import logging
import os
from enum import Enum
from typing import Annotated

import typer
from rich import print
from typer import Option

from extractload.app import lambda_handler
from extractload.clients import get_stepfunctions_client
from extractload.config import Env
from extractload.warehouse.sf_csv import SnowflakeWarehouse
from extractload.warehouse.tasks import ExtractLoadJob
from extractload.warehouse.tasks import ScheduleTask

cli = typer.Typer(
    pretty_exceptions_enable=False
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

aws_account_id = os.environ.get('AWS_ACCOUNT_ID')


class Modes(str, Enum):
    """
    Execution mode.
    """
    remote = 'remote'
    local = 'local'


def get_warehouse(env: Env) -> SnowflakeWarehouse:
    os.environ['APP_ENV'] = env.value
    return SnowflakeWarehouse.from_yaml_file()


def get_extract_load_job(schema: str, table: str, env: Env):
    warehouse = get_warehouse(env)
    return ExtractLoadJob.from_table(warehouse[schema][table])


@cli.command(name='clean')
def cli_clean(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean S3 Snowflake stage.
    """
    task = get_extract_load_job(schema, table, env).clean
    response = lambda_handler(task.as_dict, None)
    print(response)


@cli.command(name='extract')
def cli_extract(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        start: Annotated[str | None, Option(help="start date")] = None,
        end: Annotated[str | None, Option(help="end date")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Extract data from source and copy to S3 Snowflake stage.
    """
    task = get_extract_load_job(schema, table, env).extract[0]
    if start:
        task.start = start
    if end:
        task.end = end
    response = lambda_handler(task.as_dict, None)
    print(response)


@cli.command(name='load')
def cli_load(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Load data from stage to table in Snowflake.
    """
    task = get_extract_load_job(schema, table, env).load
    response = lambda_handler(task.as_dict, None)
    print(response)


@cli.command(name='schedule')
def cli_schedule():
    """
    Print schedule for extract and load tasks to console for testing.
    """
    task = ScheduleTask(database_name='sources')
    response = lambda_handler(task.as_dict, None)
    print(response)


@cli.command(name='run')
def cli_run(
        schema: Annotated[list[str] | None, Option(help="schema")] = None,
        table: Annotated[list[str] | None, Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        mode: Annotated[Modes, Option(help="local or remote execution")] = Modes.local,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
):
    """
    Run extract and load tasks. If no schema and table are provided, all tasks will be executed.
    """
    warehouse = get_warehouse(env)
    schedule_task = ScheduleTask(
        database_name=warehouse.database_name,
        schema_names=schema or [],
        table_names=table or [],
        limit=limit,
    )
    if mode == Modes.local:
        for job in schedule_task(warehouse).schedule:
            lambda_handler(job.clean.as_dict, None)
            for task in job.extract:
                lambda_handler(task.as_dict, None)
            lambda_handler(job.load.as_dict, None)
    else:
        extract_load_arn = f'arn:aws:states:eu-central-1:{aws_account_id}:stateMachine:ExtractLoadJobQueue-{env.value}'
        response_sf = get_stepfunctions_client().start_execution(
            stateMachineArn=extract_load_arn,
            input=json.dumps(schedule_task.as_dict)
        )
        print(response_sf)


@cli.command(name='tables')
def cli_tables(
        schema: Annotated[list[str] | None, Option(help="schema")] = None,
        table: Annotated[list[str] | None, Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        overwrite: bool = False,
):
    """
    Create tables in Snowflake according to src/extractload-pkg/tables.yaml.
    """
    warehouse = SnowflakeWarehouse.from_yaml_file()
    warehouse.env = env
    warehouse.create_tables(schema_names=schema, table_names=table, overwrite=overwrite, save_files=True)


if __name__ == '__main__':
    cli()

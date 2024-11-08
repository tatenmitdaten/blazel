import json
import logging
import os
from enum import Enum
from typing import Annotated
from typing import Optional

import typer
from rich import print
from typer import Option

import extractload.tables
from extractload.app import Action
from extractload.app import ExtractLoadTask
from extractload.app import ScheduleBaseTask
from extractload.app import lambda_handler
from extractload.clients import get_stepfunctions_client
from extractload.config import aws_account_id
from extractload.tables import DbWarehouse

cli = typer.Typer(
    pretty_exceptions_enable=False
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class Modes(str, Enum):
    """
    Execution mode.
    """
    remote = 'remote'
    local = 'local'


class Env(str, Enum):
    """
    Environment.
    """
    dev = 'dev'
    prod = 'prod'


@cli.command(name='clean')
def cli_clean(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean S3 Snowflake stage.
    """
    task = ExtractLoadTask(
        action=Action.cleanup,
        database_name=extractload.tables.get_database_name(env),
        schema_name=schema,
        table_name=table,
    )
    response = lambda_handler(task.model_dump(), None)
    print(response)


@cli.command(name='extract')
def cli_extract(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Extract warehouse_serialized from source and copy to S3 Snowflake stage.
    """
    task = ExtractLoadTask(
        action=Action.extract,
        database_name=extractload.tables.get_database_name(env),
        schema_name=schema,
        table_name=table,
        use_tunnel=True,
    )
    response = lambda_handler(task.model_dump(), None)
    print(response)


@cli.command(name='load')
def cli_load(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Load warehouse_serialized from stage to table in Snowflake.
    """
    task = ExtractLoadTask(
        action=Action.load,
        database_name=extractload.tables.get_database_name(env),
        schema_name=schema,
        table_name=table,
    )
    response = lambda_handler(task.model_dump(), None)
    print(response)


@cli.command(name='schedule')
def cli_schedule(
        schema: Annotated[list[str], Option(help="schema")] = None,
        table: Annotated[list[str], Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Print schedule for extract and load tasks to console for testing.
    """
    schedule_task = ScheduleBaseTask(
        database_name=extractload.tables.get_database_name(env),
        schema_names=schema,
        table_names=table,
    )
    print(schedule_task)
    print(schedule_task.schedule)


@cli.command(name='run')
def cli_run(
        schema: Annotated[list[str], Option(help="schema")] = None,
        table: Annotated[list[str], Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        mode: Annotated[Modes, Option(help="local or remote execution")] = Modes.local,
        limit: Annotated[Optional[int], Option(help="limit number of rows to extract")] = None,
):
    """
    Run extract and load tasks. If no schema and table are provided, all tasks will be executed.
    """
    os.environ['APP_ENV'] = env.value
    schedule_task = ScheduleBaseTask(
        database_name=extractload.tables.get_database_name(env),
        schema_names=schema,
        table_names=table,
        limit=limit,
        use_tunnel=True,
    )
    if mode == Modes.local:
        for job in schedule_task.schedule:
            lambda_handler(job['cleanup'], None)
            for task in job['extract']:
                lambda_handler(task, None)
            lambda_handler(job['load'], None)
    else:
        schedule_task.use_tunnel = False
        extract_load_arn = f'arn:aws:states:eu-central-1:{aws_account_id}:stateMachine:ExtractLoadJobQueue-{env.value}'
        response_sf = get_stepfunctions_client().start_execution(
            stateMachineArn=extract_load_arn,
            input=json.dumps(schedule_task.model_dump())
        )
        print(response_sf)


@cli.command(name='tables')
def cli_tables(
        schema: Annotated[list[str], Option(help="schema")] = None,
        table: Annotated[list[str], Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        overwrite: bool = False,
):
    """
    Create tables in Snowflake according to src/extractload-pkg/tables.yaml.
    """
    table = table or []
    warehouse = DbWarehouse.from_yaml_file(extractload.tables.get_database_name(env))
    warehouse.create_tables(schema_names=schema, table_names=table, overwrite=overwrite, save_files=True)


if __name__ == '__main__':
    cli()

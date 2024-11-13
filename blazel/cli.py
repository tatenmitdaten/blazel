import json
import logging
import os
from enum import Enum
from typing import Annotated

import typer
from rich import print
from typer import Option

from blazel.clients import get_stepfunctions_client
from blazel.config import Env
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import Schedule
from blazel.tasks import ScheduleTask

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


class Warehouse(SnowflakeWarehouse):
    _instance: SnowflakeWarehouse | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = SnowflakeWarehouse.from_yaml_file()
        if 'env' in kwargs and isinstance(kwargs['env'], Env):
            cls._instance.env = kwargs['env']
        return cls._instance


def get_extract_load_job(schema: str, table: str, env: Env):
    return ExtractLoadJob.from_table(Warehouse(env)[schema][table])


@cli.command(name='clean')
def cli_clean(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean S3 Snowflake stage.
    """
    get_extract_load_job(schema, table, env).clean(Warehouse(env=env))


@cli.command(name='extract')
def cli_extract(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        start: Annotated[str | None, Option(help="start date")] = None,
        end: Annotated[str | None, Option(help="end date")] = None,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Extract data from source and copy to S3 Snowflake stage.
    """
    task = get_extract_load_job(schema, table, env).extract[0]
    task.limit = limit
    if start:
        task.start = start
    if end:
        task.end = end
    task(Warehouse(env=env))


@cli.command(name='load')
def cli_load(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Load data from stage to table in Snowflake.
    """
    get_extract_load_job(schema, table, env).load(Warehouse(env=env))


@cli.command(name='schedule')
def cli_schedule():
    """
    Print schedule for extract and load tasks to console for testing.
    """
    task: ScheduleTask = ScheduleTask(database_name='blazel')
    print(task.as_dict)
    schedule = task(Warehouse(env=Env.dev))
    print(schedule)


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
    warehouse = Warehouse(env=env)
    if mode == Modes.local:
        tables = warehouse.filter(schema_names=schema, table_names=table)
        schedule = Schedule.from_tables(tables, limit=limit)
        for job in schedule.schedule:
            job.clean(warehouse)
            for task in job.extract:
                task(warehouse)
            job.load(warehouse)
    else:
        task = ScheduleTask(
            database_name=warehouse.database_name,
            schema_names=schema,
            table_names=table,
            limit=limit,
        )
        aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
        extract_load_arn = f'arn:aws:states:eu-central-1:{aws_account_id}:stateMachine:ExtractLoadJobQueue-{env.value}'
        response_sf = get_stepfunctions_client().start_execution(
            stateMachineArn=extract_load_arn,
            input=task.as_json,
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
    Warehouse(env=env).create_tables(schema_names=schema, table_names=table, overwrite=overwrite, save_files=True)


if __name__ == '__main__':
    cli()

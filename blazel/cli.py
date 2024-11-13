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


def get_extract_load_job(schema: str, table: str, env: Env):
    global warehouse
    warehouse.env = env
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
    get_extract_load_job(schema, table, env).clean()


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
    response = task
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
    get_extract_load_job(schema, table, env).load()


@cli.command(name='schedule')
def cli_schedule():
    """
    Print schedule for extract and load tasks to console for testing.
    """
    schedule = ScheduleTask(database_name='sources')(warehouse)
    print(schedule.as_dict)


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
    schedule_task = ScheduleTask(
        database_name=warehouse.database_name,
        schema_names=schema,
        table_names=table,
        limit=limit,
    )
    if mode == Modes.local:
        for job in schedule_task(warehouse).schedule:
            job.clean(warehouse)
            for task in job.extract:
                task(warehouse)
            job.load(warehouse)
    else:
        aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
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
    warehouse.env = env
    warehouse.create_tables(schema_names=schema, table_names=table, overwrite=overwrite, save_files=True)


warehouse = SnowflakeWarehouse.from_yaml_file()

if __name__ == '__main__':
    cli()

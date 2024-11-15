import logging
import os
from enum import Enum
from typing import Annotated
from typing import cast

import typer
from typer import Option

from blazel.clients import get_stepfunctions_client
from blazel.config import Env
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import ExtractTask
from blazel.tasks import Schedule
from blazel.tasks import ScheduleTask
from blazel.tasks import TaskOptions

cli = typer.Typer(
    pretty_exceptions_enable=False
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class Mode(str, Enum):
    """
    Execution mode.
    """
    remote = 'remote'
    local = 'local'


class Warehouse(SnowflakeWarehouse):
    """
    Singleton warehouse.

    The singleton allows to register extract functions before executing the CLI commands.
    """
    _instance: SnowflakeWarehouse | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = SnowflakeWarehouse.from_yaml_file()
        return cls._instance


@cli.command(name='clean')
def cli_clean(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean S3 Snowflake stage.
    """
    Env.set(env)
    wh = Warehouse()
    ExtractLoadJob.from_table(wh[schema][table]).clean(wh)


@cli.command(name='extract')
def cli_extract(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Extract data from source and copy to S3 Snowflake stage.
    """
    Env.set(env)
    wh = Warehouse()
    task = cast(ExtractTask, ExtractLoadJob.from_table(wh[schema][table]).extract[0])
    task.options = TaskOptions(
        start=start,
        end=end,
        limit=limit,
    )
    task(Warehouse())


@cli.command(name='load')
def cli_load(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Load data from stage to table in Snowflake.
    """
    Env.set(env)
    wh = Warehouse()
    ExtractLoadJob.from_table(wh[schema][table]).load(wh)


@cli.command(name='schedule')
def cli_schedule(
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Print schedule for extract and load tasks to console for testing.
    """
    from rich import print
    Env.set(env)
    task: ScheduleTask = ScheduleTask()
    schedule = task(Warehouse())
    print(schedule)


def start_statemachine(name: str, payload: str | None = None):
    try:
        aws_account_id = os.environ['AWS_ACCOUNT_ID']
    except KeyError:
        raise KeyError('AWS_ACCOUNT_ID environment variable not set')
    aws_region = os.environ.get('AWS_REGION', 'eu-central-1')
    state_machine_arn = f'arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:{name}-{Env.get().value}'
    response = get_stepfunctions_client().start_execution(
        stateMachineArn=state_machine_arn,
        input=payload or '{}',
    )
    execution_arn = response['executionArn']
    execution_link = f'https://{aws_region}.console.aws.amazon.com/states/home?region={aws_region}#/v2/executions/details/{execution_arn}'
    print(execution_link)


@cli.command(name='run')
def cli_run(
        schema: Annotated[list[str] | None, Option(help="schema")] = None,
        table: Annotated[list[str] | None, Option(help="table")] = None,
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        mode: Annotated[Mode, Option(help="local or remote execution")] = Mode.local,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
):
    """
    Run extract and load tasks. If no schema and table are provided, all tasks will be executed.
    """
    Env.set(env)
    options = TaskOptions(
        start=start,
        end=end,
        limit=limit,
    )
    if mode == Mode.local:
        warehouse = Warehouse()
        tables = warehouse.filter(schema_names=schema, table_names=table)
        schedule = Schedule.from_tables(tables, options)
        for job in schedule.schedule:
            job.clean(warehouse)
            for task in job.extract:
                task(warehouse)
            job.load(warehouse)
    else:
        start_statemachine(
            'ExtractLoadJobQueue',
            ScheduleTask(
                schema_names=schema,
                table_names=table,
                options=options
            ).as_json
        )


@cli.command(name='pipeline')
def cli_pipeline(
        env: Annotated[Env, Option(help="target environment")] = Env.dev
):
    """
    Starts the pipeline state machine.
    """
    Env.set(env)
    start_statemachine('Pipeline')


@cli.command(name='tables')
def cli_tables(
        schema: Annotated[list[str] | None, Option(help="schema")] = None,
        table: Annotated[list[str] | None, Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        overwrite: Annotated[bool, Option(help="overwrite existing tables")] = False,
        save_files: Annotated[bool, Option(help="save create table statements to sql/ folder")] = False,
):
    """
    Create tables in Snowflake according to src/extractload-pkg/tables.yaml.
    """
    Env.set(env)
    Warehouse().create_tables(
        schema_names=schema,
        table_names=table,
        overwrite=overwrite,
        save_files=save_files
    )


if __name__ == '__main__':
    cli()

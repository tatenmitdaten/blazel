import json
import logging
import os
from typing import Annotated
from typing import cast

import boto3
import click
import rich
import rich.table
import typer
from typer import Option

from blazel.config import Env
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import ExtractTask
from blazel.tasks import Schedule
from blazel.tasks import ScheduleTask
from blazel.tasks import TaskOptions

cli = typer.Typer(
    add_completion=False,
    pretty_exceptions_enable=False
)
test = typer.Typer()
cli.add_typer(test, name="test", help="Test clean, extract and load tasks")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


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


@test.command(name='clean')
def cli_clean(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean staging bucket
    """
    Env.set(env)
    wh = Warehouse()
    response = ExtractLoadJob.from_table(wh[schema][table]).clean(wh)
    rich.print(response)


@test.command(name='extract')
def cli_extract(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Extract data and copy to staging bucket
    """
    Env.set(env)
    wh = Warehouse()
    task = cast(ExtractTask, ExtractLoadJob.from_table(wh[schema][table]).extract[0])
    task.options = TaskOptions(
        start=start,
        end=end,
        limit=limit,
    )
    response = task(Warehouse())
    rich.print(response)


@test.command(name='load')
def cli_load(
        schema: Annotated[str, Option(help="schema")],
        table: Annotated[str, Option(help="table")],
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Load data from staging bucket to Snowflake
    """
    Env.set(env)
    wh = Warehouse()
    response = ExtractLoadJob.from_table(wh[schema][table]).load(wh)
    rich.print(response)


@test.command(name='schedule')
def cli_schedule(
        schema: Annotated[list[str] | None, Option(help="schema or all schemas if not provided")] = None,
        table: Annotated[list[str] | None, Option(help="table or all tables in schema if not provided")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Print default schedule to console
    """
    Env.set(env)
    task: ScheduleTask = ScheduleTask(
        schema_names=schema,
        table_names=table,
    )
    schedule = task(Warehouse())
    rich.print(schedule)


def start_statemachine(name: str, payload: str | None = None):
    try:
        aws_account_id = os.environ['AWS_ACCOUNT_ID']
    except KeyError:
        raise KeyError('AWS_ACCOUNT_ID environment variable not set')
    aws_region = os.environ.get('AWS_REGION', 'eu-central-1')
    state_machine_arn = f'arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:{name}-{Env.get().value}'
    response = boto3.client('stepfunctions').start_execution(
        stateMachineArn=state_machine_arn,
        input=payload or '{}',
    )
    execution_arn = response['executionArn']
    execution_link = f'https://{aws_region}.console.aws.amazon.com/states/home?region={aws_region}#/v2/executions/details/{execution_arn}'
    print(execution_link)


def invoke_lambda_function(name: str, payload: dict | str):
    from rich import print
    env = os.environ.get('APP_ENV', 'dev')
    aws_region = os.environ.get('AWS_REGION', 'eu-central-1')
    try:
        aws_account_id = os.environ['AWS_ACCOUNT_ID']
    except KeyError:
        raise KeyError('AWS_ACCOUNT_ID environment variable not set')
    function_arn = f'arn:aws:lambda:{aws_region}:{aws_account_id}:function:{name}-{env}'
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    response = boto3.client('lambda').invoke(
        FunctionName=function_arn,
        Payload=payload or '{}',

    )
    payload = json.loads(response['Payload'].read())
    print(payload)


@cli.command(name='run')
def cli_run(
        schema: Annotated[list[str] | None, Option(help="schema or all schemas if not provided")] = None,
        table: Annotated[list[str] | None, Option(help="table or all tables in schema if not provided")] = None,
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        remote: Annotated[bool, Option(help="local or remote execution")] = False,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
):
    """
    Schedule and run extract load jobs
    """
    Env.set(env)
    options = TaskOptions(
        start=start,
        end=end,
        limit=limit,
    )
    if remote:
        start_statemachine(
            'ExtractLoadJobQueue',
            ScheduleTask(
                schema_names=schema,
                table_names=table,
                options=options
            ).as_json
        )
    else:
        warehouse = Warehouse()
        tables = warehouse.filter(schema_names=schema, table_names=table)
        schedule = Schedule.from_tables(tables, options)
        for job in schedule.schedule:
            rich.print(job.clean(warehouse))
            for task in job.extract:
                rich.print(task(warehouse))
            rich.print(job.load(warehouse))


@cli.command(name='timestamps')
def cli_timestamps(
        schema: Annotated[list[str] | None, Option(help="schema or all schemas if not provided")] = None,
        table: Annotated[list[str] | None, Option(help="table or all tables in schema if not provided")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev
):
    """
    Update timestamps in DynamoDb
    """
    Env.set(env)
    for table in Warehouse().filter(schema_names=schema, table_names=table):
        cast(SnowflakeTable, table).update_timestamp_field()


@cli.command(name='pipeline')
def cli_pipeline(
        env: Annotated[Env, Option(help="target environment")] = Env.dev
):
    """
    Run extract load transform pipeline
    """
    Env.set(env)
    start_statemachine('Pipeline')


@cli.command(name='file')
def cli_file(
        schema_name: Annotated[str, Option('--schema', help="schema")],
        table_name: Annotated[str, Option('--table', help="table")],
        batch_number: Annotated[int, Option('-b', '--batch', help="batch number")] = 0,
        file_number: Annotated[int, Option('-f', '--file', help="file number")] = 1,
        line: Annotated[int, Option('-l', '--line', help="line number")] = 1,
        n: Annotated[int, Option('-n', '--n', help="number of lines to display")] = 10,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        style: Annotated[
            str, Option(click_type=click.Choice(['raw', 'json', 'csv']), help="display raw data")
        ] = 'raw',
):
    """
    Download and display file from Snowflake stage
    """
    Env.set(env)
    table: SnowflakeTable = Warehouse()[schema_name][table_name]
    data = table.download_from_stage(batch_number, file_number, style == 'raw')
    if line > len(data):
        print(f'Line {line} is out of range. The file has {len(data)} line(s).')
        return
    index = range(line - 1, min(line - 1 + n, len(data)))
    output: rich.table.Table | dict
    match style:
        case 'json':
            output = {i: dict(zip(table.columns, data[i])) for i in index}
        case 'csv':
            output = rich.table.Table('line', *table.columns, title=table.table_name)
            for i in index:
                output.add_row(str(i + 1), *[str(item) for item in data[i]])  # type: ignore
        case 'raw':
            output = rich.table.Table('line', 'raw', title=table.table_name)
            for i in index:
                output.add_row(str(i + 1), data[i])  # type: ignore
    rich.print(output)


@cli.command(name='tables')
def cli_tables(
        schema: Annotated[list[str] | None, Option(help="schema")] = None,
        table: Annotated[list[str] | None, Option(help="table")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        overwrite: Annotated[bool, Option(help="overwrite existing tables")] = False,
        save_files: Annotated[bool, Option(help="save create table statements to sql/ folder")] = False,
):
    """
    Create tables in Snowflake
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

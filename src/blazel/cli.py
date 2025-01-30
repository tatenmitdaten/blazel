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
from snowflake.connector import ProgrammingError
from typer import Option

from blazel.config import Env
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import Schedule
from blazel.tasks import ScheduleTask
from blazel.tasks import TaskOptions
from blazel.tasks import TimeRange

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
        schema_names: Annotated[
            list[str] | None, Option('--schema', help="schema or all schemas if not provided")
        ] = None,
        table_names: Annotated[
            list[str] | None, Option('--table', help="table or all tables in schema if not provided")
        ] = None,
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
    for table in wh.filter(schema_names=schema_names, table_names=table_names):
        batches = TimeRange(start, end).get_batch_n() if table.options.timestamp_key else 1
        options = TaskOptions(
            start=start,
            end=end,
            limit=limit,
            batches=batches
        )
        for task in ExtractLoadJob.from_table(table, options).extract:
            response = task(wh)
            rich.print(response)


@test.command(name='load')
def cli_load(
        schema_names: Annotated[
            list[str] | None, Option('--schema', help="schema or all schemas if not provided")
        ] = None,
        table_names: Annotated[
            list[str] | None, Option('--table', help="table or all tables in schema if not provided")
        ] = None,
        table_prefix: Annotated[str | None, Option(help="table prefix")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
        stop_on_error: Annotated[bool, Option(help="stop on error")] = True,
):
    """
    Load data from staging bucket to Snowflake
    """
    Env.set(env)
    wh = Warehouse()
    for table in wh.filter(schema_names=schema_names, table_names=table_names):
        if not table.table_name.startswith(table_prefix):
            continue
        try:
            response = ExtractLoadJob.from_table(table).load(wh)
            rich.print(response)
        except ProgrammingError as e:
            print(e)
            if stop_on_error:
                raise


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
        table_prefix: Annotated[str | None, Option(help="table prefix")] = '',
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
        tables = [
            table for table in warehouse.filter(schema_names=schema, table_names=table)
            if table.table_name.startswith(table_prefix)
        ]
        schedule = Schedule.from_tables(tables, options)
        for job in schedule.schedule:
            print(f'\n\033[96mProcessing "{job.clean.table_uri}"\033[0m')
            rich.print(job.clean(warehouse))
            for task in job.extract:
                rich.print(f'Extract options: {task.options}')
                rich.print(task(warehouse))
            rich.print(job.load(warehouse))


@cli.command(name='timestamps')
def cli_timestamps(
        schema: Annotated[list[str] | None, Option(help="schema or all schemas if not provided")] = None,
        table: Annotated[list[str] | None, Option(help="table or all tables in schema if not provided")] = None,
        value: Annotated[str | None, Option(help="timestamp value")] = None,
        env: Annotated[Env, Option(help="target environment")] = Env.dev
):
    """
    Update timestamps in DynamoDb
    """
    Env.set(env)
    for table in Warehouse().filter(schema_names=schema, table_names=table):
        cast(SnowflakeTable, table).update_timestamp_field(value)


def get_transform_payload(transform: list[str], env: Env) -> list[list[str]]:
    dbt = []
    for cmd in transform:
        match cmd:
            case 'build':
                dbt.append(['build', '--target', env.value])
            case 'test':
                dbt.append(['run', '--target', 'dev', '--vars', 'materialized: view'])
            case 'docs':
                dbt.append(['docs', 'generate'])
            case 'skip':
                pass
    return dbt


@cli.command(name='pipeline')
def cli_pipeline(
        schema_names: Annotated[
            list[str] | None, Option(
                '--schema', '-s',
                help='schema or all schemas if not provided'
            )
        ] = None,
        table_names: Annotated[
            list[str] | None, Option(
                '--table', '-t',
                help='table or all tables in schema if not provided'
            )
        ] = None,
        skip_extract_load: Annotated[bool, Option(help="skip extract load step")] = False,
        transform: Annotated[
            list[str], Option(
                '--transform',
                click_type=click.Choice(['build', 'test', 'docs']),
                help="transform steps to run"
            )
        ] = ('build', 'docs'),
        skip_transform: Annotated[bool, Option(help="skip transform step")] = False,
        skip_refresh: Annotated[bool, Option(help="skip dataset refresh")] = False,
        dry_run: Annotated[bool, Option(help="dry run")] = False,
        env: Annotated[Env, Option(help="target environment")] = Env.dev,

):
    """
    Run extract load transform pipeline
    """
    Env.set(env)
    payload = {}
    if not skip_extract_load:
        payload['schedule'] = ScheduleTask(
            schema_names=schema_names,
            table_names=table_names,
        ).as_dict
    if not skip_transform:
        payload['transform'] = get_transform_payload(transform, env)
    if not skip_refresh:
        payload['refresh'] = True
    print(payload)
    if not dry_run:
        start_statemachine('Pipeline', json.dumps(payload))


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
    output: rich.table.Table | dict = {}
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

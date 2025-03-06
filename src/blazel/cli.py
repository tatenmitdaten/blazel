import csv
import io
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
from blazel.config import Env
from blazel.tables import default_csv_config
from blazel.tables import SnowflakeTable
from blazel.tables import SnowflakeWarehouse
from blazel.tasks import ExtractLoadJob
from blazel.tasks import Schedule
from blazel.tasks import ScheduleTask
from blazel.tasks import TaskOptions
from blazel.tasks import TimeRange
from snowflake.connector import ProgrammingError
from typer import Option

cli = typer.Typer(
    add_completion=False,
    pretty_exceptions_enable=False
)
test = typer.Typer()
cli.add_typer(test, name="test", help="Test clean, extract and load tasks")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

schema_names_ann = Annotated[
    list[str] | None, Option(
        '--schema', '-s',
        help='schema or all schemas if not provided'
    )
]
table_names_ann = Annotated[
    list[str] | None, Option(
        '--table', '-t',
        help='table or all tables in schema if not provided'
    )
]
table_prefix_ann = Annotated[
    str | None, Option(
        '--prefix', '-p',
        help='table prefix'
    )
]
table_prefix_filter_ann = Annotated[
    str | None, Option(
        '--filter', '-f',
        click_type=click.Choice(['before', 'after', 'match']),
        help='table prefix filter type'
    )
]
stop_on_error_ann = Annotated[
    bool, Option(
        help="stop on error"
    )
]
env_ann = Annotated[
    Env, Option(
        '--env', '-e',
        help="target environment"
    )
]


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


def get_filtered_tables(
        schema_names: list[str],
        table_names: list[str],
        table_prefix: str | None,
        table_prefix_filter: str = 'match'
):
    for table in Warehouse().filter(schema_names=schema_names, table_names=table_names):
        if table_prefix:
            match table_prefix_filter:
                case 'before':
                    if table.table_name >= table_prefix:
                        continue
                case 'after':
                    if table.table_name <= table_prefix:
                        continue
                case 'match':
                    if not table.table_name.startswith(table_prefix):
                        continue
        yield table


@test.command(name='clean')
def cli_clean(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        table_prefix: table_prefix_ann = None,
        table_prefix_filter: table_prefix_filter_ann = 'match',
        env: Annotated[Env, Option(help="target environment")] = Env.dev,
):
    """
    Clean staging bucket
    """
    Env.set(env)
    for table in get_filtered_tables(schema_names, table_names, table_prefix, table_prefix_filter):
        response = ExtractLoadJob.from_table(table).clean(Warehouse())
        rich.print(response)


@test.command(name='extract')
def cli_extract(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        table_prefix: table_prefix_ann = None,
        table_prefix_filter: table_prefix_filter_ann = 'match',
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
        env: env_ann = Env.dev,
):
    """
    Extract data and copy to staging bucket
    """
    Env.set(env)
    for table in get_filtered_tables(schema_names, table_names, table_prefix, table_prefix_filter):
        batches = TimeRange(start, end).get_batch_n() if table.options.timestamp_key else 1
        options = TaskOptions(
            start=start,
            end=end,
            limit=limit,
            batches=batches
        )
        for task in ExtractLoadJob.from_table(table, options).extract:
            response = task(Warehouse())
            rich.print(response)


@test.command(name='load')
def cli_load(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        table_prefix: table_prefix_ann = None,
        table_prefix_filter: table_prefix_filter_ann = 'match',
        stop_on_error: stop_on_error_ann = True,
        env: env_ann = Env.dev,
):
    """
    Load data from staging bucket to Snowflake
    """
    Env.set(env)
    for table in get_filtered_tables(schema_names, table_names, table_prefix, table_prefix_filter):
        try:
            response = ExtractLoadJob.from_table(table).load(Warehouse())
            rich.print(response)
        except ProgrammingError as e:
            print(e)
            if stop_on_error:
                raise


@test.command(name='schedule')
def cli_schedule(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        env: env_ann = Env.dev,
):
    """
    Print default schedule to console
    """
    Env.set(env)
    task: ScheduleTask = ScheduleTask(
        schema_names=schema_names,
        table_names=table_names,
        options=TaskOptions(start=start, end=end)
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
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        table_prefix: table_prefix_ann = None,
        table_prefix_filter: table_prefix_filter_ann = 'match',
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
        remote: Annotated[bool, Option(help="local or remote execution")] = False,
        limit: Annotated[int, Option(help="limit number of rows to extract")] = 0,
        stop_on_error: stop_on_error_ann = True,
        env: env_ann = Env.dev,
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
                schema_names=schema_names,
                table_names=table_names,
                options=options
            ).as_json
        )
    else:
        warehouse = Warehouse()
        tables = get_filtered_tables(schema_names, table_names, table_prefix, table_prefix_filter)
        schedule = Schedule.from_tables(tables, options)
        for job in schedule.schedule:
            print(f'\n\033[96mProcessing "{job.clean.table_uri}"\033[0m')
            rich.print(job.clean(warehouse))
            for task in job.extract:
                rich.print(f'Extract options: {task.options}')
                rich.print(task(warehouse))
            try:
                rich.print(job.load(warehouse))
            except ProgrammingError as e:
                print(e)
                if stop_on_error:
                    raise



@cli.command(name='timestamps')
def cli_timestamps(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        value: Annotated[str | None, Option(help="timestamp value")] = None,
        env: env_ann = Env.dev
):
    """
    Update timestamps in DynamoDb
    """
    Env.set(env)
    for table in Warehouse().filter(schema_names=schema_names, table_names=table_names):
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
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        start: Annotated[str | None, Option(help="start date or datetime")] = None,
        end: Annotated[str | None, Option(help="end date or datetime")] = None,
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
        env: env_ann = Env.dev,

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
            options=TaskOptions(start=start, end=end)
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
        file_number: Annotated[int, Option('-f', '--file', help="file number")] = 0,
        line: Annotated[int, Option('-l', '--line', help="line number")] = 1,
        n: Annotated[int, Option('-n', '--n', help="number of lines to display")] = 10,
        env: env_ann = Env.dev,
        format_name: Annotated[
            str, Option(
                '--format',
                click_type=click.Choice(['raw', 'json', 'csv', 'table']),
                help="display raw data"
            )
        ] = 'raw',
        delimiter: str = default_csv_config.delimiter,
        quotechar: str = default_csv_config.quotechar,
        quoting: str = default_csv_config.quoting,
        escapechar: str = default_csv_config.escapechar,
        lineterminator: str = default_csv_config.lineterminator
):
    """
    Download and display file from Snowflake stage
    """
    Env.set(env)
    table: SnowflakeTable = Warehouse()[schema_name][table_name]
    csv_str = table.download_from_stage(batch_number, file_number)
    delimiter = delimiter.encode().decode('unicode_escape')
    lineterminator = lineterminator.encode().decode('unicode_escape')

    if format_name == 'raw':
        data = csv_str.split(lineterminator)
    else:
        if len(delimiter) == 1 and len(lineterminator) == 1:
            data = list(csv.reader(
                io.StringIO(csv_str),
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=quoting,
                escapechar=escapechar,
                lineterminator=lineterminator
            ))
        else:
            data = [line.split(delimiter) for line in csv_str.split(lineterminator)]

    if line > len(data):
        print(f'Line {line} is out of range. The file has {len(data)} line(s).')
        return
    index = range(line - 1, min(line - 1 + n, len(data)))
    output: rich.table.Table | dict | str = {}
    match format_name:
        case 'json':
            output = {
                i: dict(zip(table.columns, data[i]))
                for i in index
            }
        case 'table':
            output = rich.table.Table('line', *table.columns, title=table.table_name)
            for i in index:
                output.add_row(str(i + 1), *[str(item) for item in data[i]])  # type: ignore
        case 'csv':
            output = f'\nheader\t{table.column_names}\n' + \
                     '\n'.join(
                         f'{i + 1}\t{data[i]}'
                         for i in index
                     )
        case 'raw':
            output = f'\nheader\t{','.join(table.column_names)}\n' + \
                     '\n'.join(
                         f'{i + 1}\t{data[i].encode('unicode_escape').decode('utf-8')}'
                         for i in index
                     )

    rich.print(output)


@cli.command(name='tables')
def cli_tables(
        schema_names: schema_names_ann = None,
        table_names: table_names_ann = None,
        env: env_ann = Env.dev,
        overwrite: Annotated[bool, Option(help="overwrite existing tables")] = False,
        save_files: Annotated[bool, Option(help="save create table statements to sql/ folder")] = False,
):
    """
    Create tables in Snowflake
    """
    Env.set(env)
    Warehouse().create_tables(
        schema_names=schema_names,
        table_names=table_names,
        overwrite=overwrite,
        save_files=save_files
    )


if __name__ == '__main__':
    cli()

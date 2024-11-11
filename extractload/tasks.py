import datetime
import json
import logging
import os
import urllib.parse
import uuid
import zoneinfo
from abc import ABC
from abc import abstractmethod
from dataclasses import MISSING
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from typing import ClassVar
from typing import Generator
from typing import TypeVar
from typing import dataclass_transform

from extractload.clients import get_job_table
from extractload.clients import get_task_table
from extractload.config import default_timestamp_format
from extractload.config import default_timezone
from extractload.wh_base import DbTable
from extractload.wh_base import DbWarehouse

logger = logging.getLogger()

SerializableType = TypeVar('SerializableType', bound='Serializable')
BaseTaskType = TypeVar('BaseTaskType', bound='BaseTask')
TableTaskType = TypeVar('TableTaskType', bound='TableTask')
ExtractTaskType = TypeVar('ExtractTaskType', bound='ExtractTask')
ExtractFunctionType = TypeVar('ExtractFunctionType', bound='ExtractFunction')
DictRow = dict[str, object]
Data = Generator[DictRow, None, None] | list[DictRow]


@dataclass_transform()
@dataclass
class Serializable:

    @property
    def as_dict(self) -> dict:
        def _as_dict(obj: object) -> object:
            match obj:
                case Serializable() as s:
                    return s.as_dict
                case list() as list_member:
                    return [_as_dict(item) for item in list_member]
                case dict() as dict_member:
                    return {k: _as_dict(v) for k, v in dict_member.items()}
                case _:
                    return obj

        obj_dict = {}
        # noinspection PyTypeChecker
        for f in fields(self):
            default = MISSING
            if f.default is not MISSING:
                default = f.default
            elif f.default_factory is not MISSING:
                default = f.default_factory()

            value = getattr(self, f.name)
            if value != default:
                obj_dict[f.name] = _as_dict(value)
        return obj_dict

    @classmethod
    def from_dict(cls: type[SerializableType], data: dict) -> SerializableType:
        init_field_names = {f.name for f in fields(cls) if f.init}
        fields_dict = {}
        for key, value in data.items():
            if key in init_field_names:
                fields_dict[key] = value
        try:
            # noinspection PyArgumentList
            obj = cls(**fields_dict)
            for f in fields(cls):
                if not f.init and f.name in data:
                    setattr(obj, f.name, data[f.name])
        except TypeError as e:
            logger.error(f'Error creating {cls.__name__} from {data}')
            raise e
        return obj

    @classmethod
    def from_json(cls: type[SerializableType], json_str: str) -> SerializableType:
        return cls.from_dict(json.loads(json_str))


@dataclass
class LambdaContext(Serializable):
    execution_env: str | None = None
    default_region: str | None = None
    function_name: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    memory_limit_in_mb: str | None = None
    log_group_name: str | None = None
    log_stream_name: str | None = None
    aws_request_id: str | None = None

    @classmethod
    def from_context(cls, context):
        lambda_context = cls(
            execution_env=os.environ.get('AWS_EXECUTION_ENV'),
            default_region=os.environ.get('AWS_DEFAULT_REGION'),
            function_name=context.function_name,
            function_version=context.function_version,
            invoked_function_arn=context.invoked_function_arn,
            memory_limit_in_mb=context.memory_limit_in_mb,
            log_group_name=context.log_group_name,
            log_stream_name=context.log_stream_name,
            aws_request_id=context.aws_request_id
        )
        return lambda_context

    @property
    def cloudwatch_link(self) -> str | None:
        if self.default_region is None or self.log_group_name is None or self.log_stream_name is None or self.aws_request_id is None:
            return None
        return self.get_cloudwatch_link(
            self.default_region,
            self.log_group_name,
            self.log_stream_name,
            self.aws_request_id
        )

    @staticmethod
    def get_cloudwatch_link(
            region: str,
            log_group_name: str,
            log_stream_name: str,
            aws_request_id: str
    ) -> str:
        """
        Generate a deep link to the specific Lambda function log in AWS CloudWatch Logs.
        """
        encoded_log_group = urllib.parse.quote(log_group_name, safe='')
        encoded_log_stream = urllib.parse.quote(log_stream_name, safe='')
        filter_pattern = urllib.parse.quote(f'"{aws_request_id}"', safe='')
        return (
            f'https://{region}.console.aws.amazon.com/cloudwatch/home'
            f'?region={region}'
            f'#logsV2:log-groups/log-group/{encoded_log_group}'
            f'/log-events/{encoded_log_stream}'
            f'?filterPattern={filter_pattern}'
        )


@dataclass
class BaseTask(Serializable):
    task_type: ClassVar[str] = field(default="BaseTask", init=False)
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex, init=False)

    def __call__(self, warehouse: DbWarehouse):
        raise NotImplementedError

    @property
    def as_dict(self) -> dict:
        obj_dict = super().as_dict
        obj_dict['task_type'] = self.task_type
        return obj_dict


@dataclass
class ErrorTask(BaseTask):
    task_type: ClassVar[str] = field(default="ErrorTask", init=False)

    def __call__(self, warehouse: DbWarehouse):
        raise RuntimeError('Test Error')


@dataclass
class TableTask(BaseTask):
    task_type: ClassVar[str] = field(default="TableTask", init=False)
    job_id: str
    database_name: str
    schema_name: str
    table_name: str

    def __post_init__(self):
        self.database_name = self.database_name.lower()
        self.schema_name = self.schema_name.lower()
        self.table_name = self.table_name.lower()

    @property
    def table_uri(self) -> str:
        return f'{self.database_name}.{self.schema_name}.{self.table_name}'

    def save(self):
        get_task_table().put_item(Item=self.as_dict)

    @classmethod
    def load(cls, task_id) -> TableTaskType:
        task_dict = get_task_table().get_item(Key={'task_id': task_id})
        return cls.from_dict(task_dict['Item'])


@dataclass
class CleanTask(TableTask):
    task_type: ClassVar[str] = field(default="CleanTask", init=False)

    def __call__(self, warehouse: DbWarehouse | None = None):
        if warehouse is None:
            # load warehouse from default yaml file
            warehouse = DbWarehouse.from_yaml_file()
        return warehouse[self.schema_name][self.table_name].clean_stage()


@dataclass
class LoadTask(TableTask):
    task_type: ClassVar[str] = field(default="LoadTask", init=False)

    def __call__(self, warehouse: DbWarehouse | None = None):
        if warehouse is None:
            # load warehouse from default yaml file
            warehouse = DbWarehouse.from_yaml_file()
        return warehouse[self.schema_name][self.table_name].load_from_stage()


@dataclass
class ExtractTask(TableTask):
    task_type: ClassVar[str] = field(default="ExtractTask", init=False)
    task_number: int = 0
    batches: int = 1
    limit: int = 0

    def __call__(self, warehouse: DbWarehouse):
        if warehouse[self.schema_name][self.table_name].extract_function is None:
            raise NotImplementedError(f'No extract function registered for {self.table_uri}')
        return warehouse[self.schema_name][self.table_name].extract_function.run(self)


@dataclass
class ExtractTaskTimeRange(ExtractTask):
    task_type: ClassVar[str] = field(default="ExtractTaskTimeRange", init=False)
    start: str | None = None
    end: str | None = None
    timezone: str = default_timezone

    @staticmethod
    def _parse_date(date: str, timezone: str) -> datetime.datetime:
        try:
            return datetime.datetime.now(tz=zoneinfo.ZoneInfo(timezone))
        except ValueError:
            raise ValueError(f'Unable to parse date "{date}". Required format: {default_timestamp_format}')

    @property
    def start_date(self) -> datetime.datetime:
        if self.start is None:
            raise ValueError('The start date is required for ExtractTaskTimeRange')
        return self._parse_date(self.start, self.timezone)

    @property
    def end_date(self) -> datetime.datetime:
        if self.end is None:
            return datetime.datetime.now(tz=zoneinfo.ZoneInfo(self.timezone)) + datetime.timedelta(days=1)
        return self._parse_date(self.end, self.timezone)


@dataclass
class ExtractTaskLookBack(ExtractTaskTimeRange):
    task_type: ClassVar[str] = field(default="ExtractTaskLookBack", init=False)
    look_back: int = 1

    def __post_init__(self):
        now = datetime.datetime.now(tz=zoneinfo.ZoneInfo(self.timezone))
        self.start = (now - datetime.timedelta(days=self.look_back)).isoformat()


class ExtractFunction:
    pass


class ExtractFunctionSimple(ExtractFunction):
    def extract(self, limit: int = 0) -> None:
        raise NotImplementedError

    def __call__(self, task: ExtractTask):
        return self.extract(task.limit)


class ExtractFunctionTimeRange(ExtractFunction):
    def extract(self, start_date: datetime.datetime, end_date: datetime.datetime, limit: int = 0) -> None:
        raise NotImplementedError

    def run(self, task: ExtractTaskTimeRange | ExtractTaskLookBack):
        return self.extract(task.start_date, task.end_date, task.limit)


class ExtractFunctionBatched(ExtractFunction):
    def extract(self, batches: int, batch_number: int, limit: int = 0) -> None:
        raise NotImplementedError

    def run(self, task: ExtractTask):
        return self.extract(task.batches, task.task_number, task.limit)


@dataclass
class ExtractTable(ABC, DbTable):
    extract_function: ExtractFunctionType | None = None

    @abstractmethod
    def create_table_stmt(self) -> str:
        pass

    @abstractmethod
    def clean_stage(self):
        pass

    @abstractmethod
    def upload_to_stage(self, data: Data):
        pass

    @abstractmethod
    def load_from_stage(self):
        pass

    def register_extract_function(self, func: ExtractFunction):
        self.extract_function = func


@dataclass
class ExtractLoadJob(Serializable):
    job_id: str
    clean: CleanTask | ErrorTask
    extract: list[ExtractTaskType | ErrorTask]
    load: LoadTask | ErrorTask

    @classmethod
    def from_dict(cls: type[SerializableType], data: dict) -> SerializableType:
        data['clean'] = TaskFactory.from_dict(data['clean'])
        data['extract'] = [TaskFactory.from_dict(task) for task in data['extract']]
        data['load'] = TaskFactory.from_dict(data['load'])
        return super().from_dict(data)

    @classmethod
    def from_table(cls, table: ExtractTable) -> 'ExtractLoadJob':
        job_id = uuid.uuid4().hex
        return cls(
            job_id=job_id,
            clean=CleanTask(
                job_id=job_id,
                database_name=table.database_name,
                schema_name=table.schema_name,
                table_name=table.table_name
            ),
            extract=[
                ExtractTask(
                    job_id=job_id,
                    database_name=table.database_name,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    task_number=task_number,
                    batches=table.options.batches,
                ) for task_number in range(table.options.batches)
            ],
            load=LoadTask(
                job_id=job_id,
                database_name=table.database_name,
                schema_name=table.schema_name,
                table_name=table.table_name,
            )
        )

    def save(self):
        item = {
            'job_id': self.job_id,
            'clean': self.clean.task_id,
            'extract': [task.task_id for task in self.extract],
            'load': self.load.task_id,
        }
        get_job_table().put_item(Item=item)

    def load(self) -> 'ExtractLoadJob':
        job_dict = get_job_table().get_item(Key={'job_id': self.job_id})['Item']
        task_table = get_task_table()
        job_dict['clean'] = task_table.get_item(Key={'task_id': job_dict['clean']})['Item']
        job_dict['extract'] = [
            task_table.get_item(Key={'task_id': task_id})['Item']
            for task_id in job_dict['extract']
        ]
        job_dict['load'] = task_table.get_item(Key={'task_id': job_dict['load']})['Item']
        return ExtractLoadJob.from_dict(job_dict)


@dataclass
class Schedule(Serializable):
    schedule: list[ExtractLoadJob] = field(default_factory=list)


@dataclass
class ScheduleTask(BaseTask):
    task_type: ClassVar[str] = field(default="ScheduleTask", init=False)
    database_name: str
    schema_names: list[str] = field(default_factory=list)
    table_names: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.schema_names = [schema_name.lower() for schema_name in self.schema_names]
        self.table_names = [table_name.lower() for table_name in self.table_names]

    def __call__(self, warehouse: DbWarehouse) -> Schedule:

        # Test Error for testing error handling
        if self.schema_names == ['error']:
            raise RuntimeError('Test Error')
        if self.table_names == ['error']:
            error_task = ErrorTask()
            return Schedule(schedule=[
                ExtractLoadJob(
                    job_id=uuid.uuid4().hex,
                    clean=error_task,
                    extract=[error_task],
                    load=error_task
                )
            ])

        tables = warehouse.filter(self.schema_names, self.table_names)
        schedule = Schedule()
        table: ExtractTable
        for table in tables:
            if table.options.ignore:
                continue
            schedule.schedule.append(
                ExtractLoadJob.from_table(table)
            )
        return schedule


class TaskFactory:
    _task_types: dict[str, BaseTaskType] = {
        'ErrorTask': ErrorTask,
        'ExtractTask': ExtractTask,
        'ExtractTaskTimeRange': ExtractTaskTimeRange,
        'ExtractTaskLookBack': ExtractTaskLookBack,
        'CleanTask': CleanTask,
        'LoadTask': LoadTask,
        'ScheduleTask': ScheduleTask,
    }

    @classmethod
    def register(cls, task_class: BaseTaskType) -> None:
        instance = task_class.__new__(task_class)
        cls._task_types[instance.task_type] = task_class

    @classmethod
    def from_dict(cls, data: dict) -> BaseTaskType:
        task_type = data.get('task_type')
        if task_type not in cls._task_types:
            raise ValueError(f"Could not find task type: {task_type}")
        return cls._task_types[task_type].from_dict(data)

    @classmethod
    def from_json(cls, json_str: str) -> BaseTaskType:
        data = json.loads(json_str)
        return cls.from_dict(data)

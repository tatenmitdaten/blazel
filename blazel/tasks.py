import datetime
import json
import logging
import uuid
import zoneinfo
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Iterator
from typing import TypeVar

from blazel.base import BaseOptions
from blazel.base import BaseSchema
from blazel.base import BaseTable
from blazel.base import BaseWarehouse
from blazel.base import get_database_name
from blazel.clients import get_job_table
from blazel.clients import get_task_table
from blazel.config import default_timestamp_format
from blazel.serializable import Serializable
from blazel.serializable import SerializableType

logger = logging.getLogger()

DictRow = dict[str, object]
Data = Generator[DictRow, None, None] | list[DictRow]
TableTaskType = TypeVar('TableTaskType', bound='TableTask')
BaseTaskType = TypeVar('BaseTaskType', bound='BaseTask')
ExtractTaskType = TypeVar('ExtractTaskType', bound='ExtractTask')
ExtractLoadTableType = TypeVar('ExtractLoadTableType', bound='ExtractLoadTable')
ExtractLoadSchemaType = TypeVar('ExtractLoadSchemaType', bound='ExtractLoadSchema')
ExtractLoadWarehouseType = TypeVar('ExtractLoadWarehouseType', bound='ExtractLoadWarehouse')
ExtractFunctionType = Callable[[ExtractLoadTableType, TableTaskType], dict | None]


@dataclass
class ExtractLoadTable(
    ABC,
    BaseTable[ExtractLoadSchemaType, ExtractLoadTableType],
    Generic[ExtractLoadSchemaType, ExtractLoadTableType, TableTaskType]
):
    extract_function: ExtractFunctionType | None = None

    @abstractmethod
    def clean_stage(self) -> dict | None:
        pass

    @abstractmethod
    def load_from_stage(self) -> dict | None:
        pass

    @abstractmethod
    def upload_to_stage(self, data: Data):
        pass

    @abstractmethod
    def get_latest_timestamp(self) -> str | None:
        pass

    def register_extract_function(self, f: ExtractFunctionType):
        self.extract_function = f


class ExtractLoadSchema(BaseSchema[ExtractLoadWarehouseType, ExtractLoadSchemaType, ExtractLoadTableType]):
    pass


class ExtractLoadWarehouse(BaseWarehouse[ExtractLoadWarehouseType, ExtractLoadSchemaType, ExtractLoadTableType]):
    pass


@dataclass
class TaskOptions(BaseOptions):
    start: str | None = None
    end: str | None = None
    batches: int = 1
    limit: int = 0


@dataclass
class BaseTask(Serializable, Generic[ExtractLoadWarehouseType]):
    task_type: str = field(default="BaseTask", init=False)
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex, init=False)

    def __call__(self, warehouse: ExtractLoadWarehouseType) -> dict | None:
        raise NotImplementedError

    @property
    def as_dict(self) -> dict:
        obj_dict = super().as_dict
        obj_dict['task_type'] = self.task_type
        return obj_dict


@dataclass
class TableTask(BaseTask[ExtractLoadWarehouseType], Generic[ExtractLoadWarehouseType, ExtractLoadTableType]):
    task_type: str = field(default="TableTask", init=False)
    job_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None

    def __post_init__(self):
        if self.job_id is None:
            raise ValueError('job_id is required')
        if self.database_name is None:
            raise ValueError('database_name is required')
        if self.schema_name is None:
            raise ValueError('schema_name is required')
        if self.table_name is None:
            raise ValueError('table_name is required')
        self.database_name = self.database_name.lower()
        self.schema_name = self.schema_name.lower()
        self.table_name = self.table_name.lower()

    @property
    def table_uri(self) -> str:
        return f'{self.database_name}.{self.schema_name}.{self.table_name}'

    def table(self, warehouse: ExtractLoadWarehouseType) -> ExtractLoadTableType:
        return warehouse[self.schema_name][self.table_name]

    def to_dynamodb(self):
        get_task_table().put_item(Item=self.as_dict)

    @classmethod
    def from_dynamodb(cls: type[TableTaskType], task_id) -> TableTaskType:
        task_dict = get_task_table().get_item(Key={'task_id': task_id})
        return cls.from_dict(task_dict['Item'])


@dataclass
class ErrorTask(TableTask):
    task_type: str = field(default="ErrorTask", init=False)

    def __call__(self, warehouse: ExtractLoadWarehouseType) -> dict | None:
        raise RuntimeError('Test Error')


@dataclass
class CleanTask(TableTask):
    task_type: str = field(default="CleanTask", init=False)

    def __call__(self, warehouse: ExtractLoadWarehouseType) -> dict | None:
        return self.table(warehouse).clean_stage()


@dataclass
class LoadTask(TableTask):
    task_type: str = field(default="LoadTask", init=False)

    def __call__(self, warehouse: ExtractLoadWarehouseType) -> dict | None:
        return self.table(warehouse).load_from_stage()


class TimeRange(Generic[ExtractLoadTableType]):
    def __init__(self, task: 'ExtractTask', table: ExtractLoadTableType):
        self.tzinfo = zoneinfo.ZoneInfo(table.options.timezone)

        start = task.options.start
        end = task.options.end
        if start is None:
            if table.options.timestamp_field:
                start = table.get_latest_timestamp()
            if table.options.look_back_days:
                interval = datetime.timedelta(days=table.options.look_back_days)
                start_date = self._get_now_timestamp() - interval
                start = start_date.strftime(default_timestamp_format)
        if start is None:
            start_date = datetime.datetime(year=1980, month=1, day=1)
            start = start_date.strftime(default_timestamp_format)
        if end is None:
            end_date = datetime.datetime(year=2100, month=12, day=31)
            end = end_date.strftime(default_timestamp_format)

        self.start = start
        self.end = end

    @property
    def start_date(self) -> datetime.datetime:
        return self._parse_date(self.start)

    @property
    def end_date(self) -> datetime.datetime:
        return self._parse_date(self.end)

    def _get_now_timestamp(self):
        return datetime.datetime.now(tz=self.tzinfo)

    @staticmethod
    def _parse_date(date: str) -> datetime.datetime:
        try:
            return datetime.datetime.strptime(date, default_timestamp_format)
        except ValueError:
            raise ValueError(f'Unable to parse date "{date}". Required format: {default_timestamp_format}')


@dataclass
class ExtractTask(TableTask):
    task_type: str = field(default="ExtractTask", init=False)
    task_number: int = 0
    options: TaskOptions = field(default_factory=TaskOptions)

    def __call__(self, warehouse: ExtractLoadWarehouseType):
        table = self.table(warehouse)
        if table.extract_function is None:
            raise ValueError(f'No extract function registered for table {self.table_uri}')
        return table.extract_function(table, self)

    def get_time_range(self, table: ExtractLoadTableType) -> 'TimeRange':
        return TimeRange(self, table)


@dataclass
class ExtractLoadJob(Serializable):
    job_id: str
    clean: CleanTask | ErrorTask
    extract: list[ExtractTask | ErrorTask]
    load: LoadTask | ErrorTask

    @classmethod
    def from_dict(cls: type[SerializableType], data: dict) -> SerializableType:
        data['clean'] = TaskFactory.from_dict(data['clean'])
        data['extract'] = [TaskFactory.from_dict(task) for task in data['extract']]
        data['load'] = TaskFactory.from_dict(data['load'])
        return super().from_dict(data)

    @classmethod
    def from_table(cls, table: ExtractLoadTable, options: TaskOptions | None = None) -> 'ExtractLoadJob':
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
                    options=options or TaskOptions(),
                ) for task_number in range(table.options.batches)
            ],
            load=LoadTask(
                job_id=job_id,
                database_name=table.database_name,
                schema_name=table.schema_name,
                table_name=table.table_name,
            )
        )

    def to_dynamodb(self):
        item = {
            'job_id': self.job_id,
            'clean': self.clean.task_id,
            'extract': [task.task_id for task in self.extract],
            'load': self.load.task_id,
        }
        get_job_table().put_item(Item=item)
        task_table = get_task_table()
        task_table.put_item(Item=self.clean.as_dict)
        for task in self.extract:
            task_table.put_item(Item=task.as_dict)
        task_table.put_item(Item=self.load.as_dict)

    @classmethod
    def from_dynamodb(cls, job_id: str) -> 'ExtractLoadJob':
        job_dict = get_job_table().get_item(Key={'job_id': job_id})['Item']  # type: dict
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

    def __iter__(self) -> Iterator[ExtractLoadJob]:
        return iter(self.schedule)

    @classmethod
    def error_schedule(cls) -> 'Schedule':
        error_task = ErrorTask()
        return Schedule(schedule=[
            ExtractLoadJob(
                job_id=uuid.uuid4().hex,
                clean=error_task,
                extract=[error_task],
                load=error_task
            )
        ])

    @classmethod
    def from_tables(cls, tables: list[ExtractLoadTableType], options: TaskOptions) -> 'Schedule':
        schedule = Schedule()
        for table in tables:
            if table.options.ignore:
                continue
            schedule.schedule.append(
                ExtractLoadJob.from_table(table, options)
            )
        return schedule


@dataclass
class ScheduleTask(BaseTask[ExtractLoadWarehouseType]):
    task_type: str = field(default="ScheduleTask", init=False)
    database_name: str | None = None
    schema_names: list[str] | None = None
    table_names: list[str] | None = None
    options: TaskOptions = field(default_factory=TaskOptions)

    def __post_init__(self):
        if self.database_name is None:
            self.database_name = get_database_name()
        if self.schema_names:
            self.schema_names = [schema_name.lower() for schema_name in self.schema_names]
        if self.table_names:
            self.table_names = [table_name.lower() for table_name in self.table_names]

    def __call__(self, warehouse: ExtractLoadWarehouseType) -> dict | None:

        # Test Error for testing error handling
        if self.schema_names == ['error']:
            raise RuntimeError('Test Error')
        elif self.table_names == ['error']:
            schedule = Schedule.error_schedule()
        else:
            tables = warehouse.filter(self.schema_names, self.table_names)
            schedule = Schedule.from_tables(tables, options=self.options)
        return schedule.as_dict


class TaskFactory(Generic[BaseTaskType]):
    _task_types: dict[str, type[BaseTask]] = {
        'ErrorTask': ErrorTask,
        'ExtractTask': ExtractTask,
        'CleanTask': CleanTask,
        'LoadTask': LoadTask,
        'ScheduleTask': ScheduleTask,
    }

    @classmethod
    def register(cls, task_class: type[BaseTaskType]) -> None:
        instance: BaseTask = task_class.__new__(task_class)
        cls._task_types[instance.task_type] = task_class

    @classmethod
    def from_dict(cls, data: dict) -> BaseTask:
        task_type = data.get('task_type')
        if task_type not in cls._task_types:
            raise ValueError(f"Could not find task type: {task_type}")
        return cls._task_types[task_type].from_dict(data)

    @classmethod
    def from_json(cls, json_str: str) -> BaseTask:
        data = json.loads(json_str)
        return cls.from_dict(data)

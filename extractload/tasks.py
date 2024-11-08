import datetime
import json
import logging
import zoneinfo
from abc import ABC
from abc import abstractmethod
from dataclasses import MISSING
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from typing import Generator
from typing import TypeVar
from typing import dataclass_transform

from config import default_timestamp_format
from config import default_timezone
from wh_base import DbTable
from wh_base import DbWarehouse

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
        def _get_default(f):
            if f.default is not MISSING:
                return f.default
            elif f.default_factory is not MISSING:
                return f.default_factory()
            return MISSING

        def _as_dict(obj: object) -> object:
            match obj:
                case Serializable() as s:
                    return s.as_dict
                case list() as list_member:
                    return [_as_dict(item) for item in list_member]
                case dict() as dict_member:
                    return {key: _as_dict(value) for key, value in dict_member.items()}
                case _:
                    return obj

        # noinspection PyTypeChecker
        return {
            f.name: _as_dict(getattr(self, f.name))
            for f in fields(self)
            if getattr(self, f.name) != _get_default(f) or f.name == 'task_type'
        }

    @classmethod
    def from_dict(cls: type[SerializableType], data: dict) -> SerializableType:
        data = data.copy()
        if 'task_type' in data:
            del data['task_type']
        try:
            # noinspection PyArgumentList
            obj = cls(**data)
        except TypeError as e:
            logger.error(f'Error creating {cls.__name__} from {data}')
            raise e
        return obj

    @classmethod
    def from_json(cls: type[SerializableType], json_str: str) -> SerializableType:
        return cls.from_dict(json.loads(json_str))


@dataclass
class BaseTask(Serializable):
    task_type: str = field(default="BaseTask", init=False)

    def __call__(self, warehouse: DbWarehouse):
        raise NotImplementedError


@dataclass
class ErrorTask(BaseTask):
    task_type: str = field(default="ErrorTask", init=False)

    def __call__(self, warehouse: DbWarehouse):
        raise RuntimeError('Test Error')


@dataclass
class TableTask(BaseTask):
    task_type: str = field(default="TableTask", init=False)
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

    @classmethod
    def from_uri(cls, uri: str) -> TableTaskType:
        database_name, schema_name, table_name = uri.split('.')
        return cls(database_name, schema_name, table_name)


@dataclass
class CleanTask(TableTask):
    task_type: str = field(default="CleanTask", init=False)

    def __call__(self, warehouse: DbWarehouse | None = None):
        if warehouse is None:
            # load warehouse from default yaml file
            warehouse = DbWarehouse.from_yaml_file()
        return warehouse[self.schema_name][self.table_name].clean_stage()


@dataclass
class LoadTask(TableTask):
    task_type: str = field(default="LoadTask", init=False)

    def __call__(self, warehouse: DbWarehouse | None = None):
        if warehouse is None:
            # load warehouse from default yaml file
            warehouse = DbWarehouse.from_yaml_file()
        return warehouse[self.schema_name][self.table_name].load_from_stage()


@dataclass
class ExtractTask(TableTask):
    task_type: str = field(default="ExtractTask", init=False)
    limit: int = 0

    def __call__(self, warehouse: DbWarehouse):
        if warehouse[self.schema_name][self.table_name].extract_function is None:
            raise NotImplementedError(f'No extract function registered for {self.table_uri}')
        return warehouse[self.schema_name][self.table_name].extract_function.run(self)


@dataclass
class ExtractTaskBatched(ExtractTask):
    task_type: str = field(default="ExtractTaskBatched", init=False)
    batches: int = 1
    batch_number: int = 0


@dataclass
class ExtractTaskTimeRange(ExtractTask):
    task_type: str = field(default="ExtractTaskTimeRange", init=False)
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
    task_type: str = field(default="ExtractTaskLookBack", init=False)
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

    def run(self, task: ExtractTaskBatched):
        return self.extract(task.batches, task.batch_number, task.limit)


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

    def create_clean_task(self) -> CleanTask:
        return CleanTask(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=self.table_name
        )

    def create_extract_tasks(self) -> list[ExtractTaskType]:
        if self.options.look_back:
            return [ExtractTaskLookBack(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                look_back=self.options.look_back,
                timezone=self.options.timezone,
            )]
        elif self.options.batches > 1:
            return [ExtractTaskBatched(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                batches=self.options.batches,
                batch_number=batch_number,
            ) for batch_number in range(self.options.batches)]
        else:
            return [ExtractTask(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
            )]

    def create_load_task(self) -> LoadTask:
        return LoadTask(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        )


@dataclass
class ExtractLoadJob(Serializable):
    clean: CleanTask | ErrorTask
    extract: list[ExtractTaskType | ErrorTask]
    load: LoadTask | ErrorTask


@dataclass
class Schedule(Serializable):
    schedule: list[ExtractLoadJob] = field(default_factory=list)


@dataclass
class ScheduleTask(BaseTask):
    task_type: str = field(default="ScheduleTask", init=False)
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
                ExtractLoadJob(
                    clean=table.create_clean_task(),
                    extract=table.create_extract_tasks(),
                    load=table.create_load_task(),
                )
            )
        return schedule


class TaskFactory:
    _task_types: dict[str, BaseTaskType] = {
        'ErrorTask': ErrorTask,
        'ExtractTask': ExtractTask,
        'ExtractTaskBatched': ExtractTaskBatched,
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
        task_name = data.get('task_type')
        if task_name not in cls._task_types:
            raise ValueError(f"Could not find task type: {task_name}")
        return cls._task_types[task_name].from_dict(data)

    @classmethod
    def from_json(cls, json_str: str) -> BaseTaskType:
        data = json.loads(json_str)
        return cls.from_dict(data)

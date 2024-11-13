import json
import logging
from dataclasses import dataclass
from dataclasses import fields
from dataclasses import MISSING
from typing import dataclass_transform
from typing import TypeVar

SerializableType = TypeVar('SerializableType', bound='Serializable')
logger = logging.getLogger(__name__)


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
            if value != default or not f.init:
                obj_dict[f.name] = _as_dict(value)
        return obj_dict

    @property
    def as_json(self) -> str:
        return json.dumps(self.as_dict, default=str)

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

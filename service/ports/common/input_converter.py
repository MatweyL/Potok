import json
from abc import ABC, abstractmethod
from typing import Any, Type, Dict

from pydantic import BaseModel


class InputConverterI(ABC):

    @abstractmethod
    def convert(self, raw_message: Any) -> Any:
        pass


class FromBytesToStr(InputConverterI):

    def __init__(self, encoding: str):
        self._encoding = encoding

    def convert(self, raw_message: bytes) -> str:
        return raw_message.decode(encoding=self._encoding)


class FromStrToDict(InputConverterI):
    def __init__(self, *converting_args, **converting_kwargs):
        self._converting_args = converting_args
        self._converting_kwargs = converting_kwargs

    def convert(self, raw_message: str) -> dict:
        return json.loads(raw_message, *self._converting_args, **self._converting_kwargs)


class FromStrOrBytesToPydantic(InputConverterI):
    def __init__(self, schema: Type[BaseModel], **converting_kwargs):
        self._schema = schema
        self._converting_kwargs = converting_kwargs

    def convert(self, raw_message: str) -> Type[BaseModel]:
        return self._schema.model_validate_json(raw_message, **self._converting_kwargs, )


class FromStrToPydanticByType(InputConverterI):
    def __init__(self, type_field_name: str, pydantic_class_by_type: Dict[str, Type[BaseModel]]):
        self._type_field_name = type_field_name
        self._pydantic_class_by_type = pydantic_class_by_type
        self._to_dict = FromStrToDict()

    def convert(self, raw_message: str) -> BaseModel:
        message_dict = self._to_dict.convert(raw_message)
        message_type = message_dict[self._type_field_name]
        pydantic_class = self._pydantic_class_by_type[message_type]
        return pydantic_class(**message_dict)


class ChainedConverter(InputConverterI):

    def __init__(self, converter: InputConverterI, next_converter: 'ChainedConverter' = None):
        self._converter = converter
        self._next_converter = next_converter

    def convert(self, raw_message: Any) -> Any:
        converted_message = self._converter.convert(raw_message)
        if self._next_converter:
            return self._next_converter.convert(converted_message)
        return converted_message

import json
from typing import Union, List, Dict

from pydantic import BaseModel

from service.ports.common.logs import logger


def dict_as_json(item: Union[Dict, List]) -> str:
    return json.dumps(item, default=str, indent=2)


def to_bytes(item: bytes | dict | str | BaseModel) -> bytes:
    try:
        if isinstance(item, bytes):
            return item
        if isinstance(item, str):
            return item.encode('utf-8')
        if isinstance(item, (dict, list)):
            return dict_as_json(item).encode('utf-8')
        if isinstance(item, BaseModel):
            return item.model_dump_json(indent=2).encode('utf-8')
    except BaseException as e:
        logger.error(e)


def to_flat_json(data: dict):
    """ {k1: {inner1: 1, inner2: 2}} -> {k1_inner1: 1, k1_inner1: 2} """
    if not _has_nested_jsons(data):
        return data
    return _to_flat_json(data)


def _has_nested_jsons(data):
    for key in data:
        if isinstance(data[key], dict):
            return True
    return False


def _to_flat_json(data: dict, parent_key: str = None) -> dict:
    flat_data = {}
    for key in data:
        new_key = f'{parent_key}_{key}' if parent_key else key
        if isinstance(data[key], dict):
            nested_flat_data = _to_flat_json(data[key], new_key)
            flat_data.update(nested_flat_data)
        else:
            flat_data[new_key] = data[key]
    return flat_data

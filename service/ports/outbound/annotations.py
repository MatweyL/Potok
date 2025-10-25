from typing import Annotated

from pydantic import BeforeValidator


def str_as_int(number: str) -> int:
    return int(number)


def make_str_lower(text: str) -> str:
    return text.lower()


before_validator_str_as_int = BeforeValidator(func=str_as_int)
before_validator_make_str_lower = BeforeValidator(func=make_str_lower)

ReducibleToInt = Annotated[int, before_validator_str_as_int]
ReducibleToLoweredStr = Annotated[str, before_validator_make_str_lower]

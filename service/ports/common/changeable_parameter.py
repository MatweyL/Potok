from typing import Optional

from pydantic import BaseModel


class BaseChangeableParameter(BaseModel):
    name: Optional[str] = None


class ChangeableBoolParameter(BaseChangeableParameter):
    value: bool


class ChangeableIntParameter(BaseChangeableParameter):
    value: int


class ChangeableFloatParameter(BaseChangeableParameter):
    value: float


class ChangeableStrParameter(BaseChangeableParameter):
    value: str

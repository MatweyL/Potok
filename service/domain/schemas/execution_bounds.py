from datetime import datetime
from typing import Optional, Union, Dict

from pydantic import BaseModel

from service.domain.schemas.enums import TaskType


class TimeIntervalBounds(BaseModel):
    type: TaskType = TaskType.TIME_INTERVAL
    right_bound_at: datetime
    left_bound_at: Optional[datetime] = None


ExecutionBounds = Union[TimeIntervalBounds]


def as_execution_bounds(obj: Dict) -> ExecutionBounds:
    obj_type = obj.get('type')
    if not obj_type:
        raise ValueError(f"No attribute type in {obj} for converting to execution bounds")
    if obj_type == TaskType.TIME_INTERVAL:
        return TimeIntervalBounds(**obj)
    raise ValueError(f"Unknown type in {obj} for converting to execution bounds")
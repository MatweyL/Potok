from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel


class TimeIntervalBounds(BaseModel):
    right_bound_at: datetime
    left_bound_at: Optional[datetime] = None


ExecutionBounds = Union[TimeIntervalBounds]

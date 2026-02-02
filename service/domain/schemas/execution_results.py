from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel


class TimeIntervalExecutionResults(BaseModel):
    right_bound_at: datetime
    left_bound_at: datetime
    collected_data_amount: Optional[int] = None
    saved_data_amount: Optional[int] = None


ExecutionResults = Union[TimeIntervalExecutionResults]

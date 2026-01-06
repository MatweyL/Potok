from typing import Optional, Union

from pydantic import BaseModel


class TimeIntervalExecutionResults(BaseModel):
    collected_data_amount: Optional[int] = None
    saved_data_amount: Optional[int] = None


ExecutionResults = Union[TimeIntervalExecutionResults]
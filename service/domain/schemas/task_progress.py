from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TimeIntervalTaskProgressPK(BaseModel):
    task_id: int
    right_bound_at: datetime

    def __eq__(self, other):
        return isinstance(other, TimeIntervalTaskProgressPK) \
               and self.task_id == other.task_id \
               and self.right_bound_at == other.right_bound_at

    def __hash__(self):
        return hash((self.task_id, self.right_bound_at))


class TimeIntervalTaskProgress(TimeIntervalTaskProgressPK):
    left_bound_at: Optional[datetime] = None
    collected_data_amount: int
    saved_data_amount: int

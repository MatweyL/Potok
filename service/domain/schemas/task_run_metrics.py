from functools import cached_property
from typing import Dict

from pydantic import BaseModel


class TaskRunGroupedAvgMetrics(BaseModel):
    group_name: str
    period_s: int

    avg_queued_duration: float = 0
    avg_execution_duration: float = 0
    avg_retry_count: float = 0  # where status in (TEMP_ERROR, WAITING, INTERRUPTED) group by status having count(*) > 1
    avg_temp_error_count: float = 0
    avg_interrupted_count: float = 0


class TaskRunAvgMetrics(BaseModel):
    grouped_avg_metrics_by_name: Dict[str, TaskRunGroupedAvgMetrics]


class TaskRunGroupedMetrics(BaseModel):
    group_name: str
    period_s: int

    waiting: int = 0
    succeed: int = 0
    temp_error: int = 0
    interrupted: int = 0
    queued: int = 0
    execution: int = 0
    cancelled: int = 0
    error: int = 0

    @cached_property
    def total(self) -> int:
        return (self.waiting + self.succeed + self.temp_error + self.interrupted
                + self.queued + self.execution + self.cancelled + self.error)

    @cached_property
    def completed(self) -> int:
        return self.succeed + self.error + self.cancelled

    @cached_property
    def failed(self):
        return self.temp_error + self.interrupted

    @cached_property
    def throughput(self):
        return self.succeed / self.period_s


class TaskRunMetrics(BaseModel):
    grouped_metrics_by_name: Dict[str, TaskRunGroupedMetrics]

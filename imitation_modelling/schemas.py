import enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Dict

from pydantic import BaseModel, Field


class TaskRunStatus(str, enum.Enum):
    WAITING = "WAITING"
    QUEUED = "QUEUED"
    EXECUTION = "EXECUTION"
    INTERRUPTED = "INTERRUPTED"
    TEMP_ERROR = "TEMP_ERROR"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    SUCCEED = "SUCCEED"


class SystemTime:

    def __init__(self, base_time_iso: str = "2025-10-12 12:00:00", time_step_seconds: float = 1.0, ):
        self._time_current = datetime.strptime(base_time_iso, "%Y-%m-%d %H:%M:%S")
        self._time_start = self._time_current
        self._time_step_seconds = timedelta(seconds=time_step_seconds)

    def tick(self):
        self._time_current += self._time_step_seconds

    @property
    def start(self):
        return self._time_start

    @property
    def current(self):
        return self._time_current


@dataclass
class TaskRun:
    id: str


@dataclass
class TaskRunStatusLog:
    """Единичное выполнение задачи"""
    task_run_id: str
    status: TaskRunStatus
    created_timestamp: datetime


@dataclass
class TaskExecution:
    task_run: TaskRun
    finish_time: datetime
    next_execution_confirm_time: datetime


class TaskBatchProviderType(str, enum.Enum):
    CONSTANT_SIZE = "CONSTANT_SIZE"
    PID = "PID"
    AIMD = "AIMD"


class SimulationParams(BaseModel):
    handlers_amount: int = 5
    handler_max_tasks: int = 4
    execution_confirm_timeout: int = 300
    tasks_part_from_all_for_high_load: float = 0.9
    temp_error_probability_at_high_load: float = 0.1
    random_timeout_generator_left: int = 25
    random_timeout_generator_right: int = 25
    tasks_amount: int = 1000
    interrupted_timeout: int = 400
    run_timeout: int = 30
    metric_provider_period: int = 150
    time_step_seconds: int = 25

    task_batch_provider_params: Dict[str, Any] = Field(default={"batch_size": 100})
    task_batch_provider_type: TaskBatchProviderType = TaskBatchProviderType.CONSTANT_SIZE
    config_file_name: str = 'default'

    max_run_seconds: int = 60

    @cached_property
    def run_name(self) -> str:
        return f"{self.config_file_name}__{self.task_batch_provider_type.value}"

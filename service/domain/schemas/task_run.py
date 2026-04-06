from datetime import datetime
from functools import cached_property
from typing import Optional, Dict, Any, List

from pydantic import BaseModel

from service.domain.schemas.enums import TaskRunStatus, PriorityType, TaskType
from service.domain.schemas.execution_bounds import ExecutionBounds, TimeIntervalBounds
from service.domain.schemas.payload import Payload


class TaskRunPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, TaskRunPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class TaskRun(TaskRunPK):
    """ Запуск задачи. Содержит всю необходимую информацию для выполнения задачи """
    task_id: int
    group_name: str
    priority: PriorityType = PriorityType.MEDIUM
    type: TaskType = TaskType.UNDEFINED
    payload: Optional[Payload] = None
    execution_bounds: Optional[ExecutionBounds] = None
    execution_arguments: Optional[Dict[str, Any]] = None

    status: TaskRunStatus
    status_updated_at: datetime
    description: Optional[str] = None

    @cached_property
    def queue_name(self):
        return f"{self.group_name}.{self.type.value}.{self.priority.value}"


class TaskRunStatusLogPK(BaseModel):
    task_run_id: int
    status_updated_at: datetime

    def __eq__(self, other):
        return isinstance(other, TaskRunStatusLogPK) and self.task_run_id == other.task_run_id

    def __hash__(self):
        return hash(self.task_run_id)


class TaskRunStatusLog(TaskRunStatusLogPK):
    status: TaskRunStatus
    description: Optional[str] = None


class TaskRunTimeIntervalExecutionBoundsPK(BaseModel):
    task_run_id: int


class TaskRunTimeIntervalExecutionBounds(TaskRunTimeIntervalExecutionBoundsPK):
    task_id: int
    execution_bounds: TimeIntervalBounds


class TaskRunTimeIntervalProgressPK(BaseModel):
    task_run_id: int
    right_bound_at: datetime

    def __eq__(self, other):
        return isinstance(other, TaskRunTimeIntervalProgressPK) and self.task_run_id == other.task_run_id and self.right_bound_at == other.right_bound_at

    def __hash__(self):
        return hash((self.task_run_id, self.right_bound_at))


class TaskRunTimeIntervalProgress(TaskRunTimeIntervalProgressPK):
    left_bound_at: Optional[datetime] = None
    collected_data_amount: int
    saved_data_amount: int


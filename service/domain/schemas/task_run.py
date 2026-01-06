from datetime import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel

from service.domain.schemas.enums import TaskRunStatus, PriorityType, TaskType
from service.domain.schemas.execution_bounds import ExecutionBounds
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
    execution_bounds: Optional[List[ExecutionBounds]] = None
    execution_arguments: Optional[Dict[str, Any]] = None

    status: TaskRunStatus
    status_updated_at: datetime
    description: Optional[str] = None


class TaskRunStatusLogPK(BaseModel):
    task_run_id: int
    status_updated_at: datetime

    def __eq__(self, other):
        return isinstance(other, TaskRunStatusLogPK) and self.task_run_id == other.task_run_id

    def __hash__(self):
        return hash(self.id)


class TaskRunStatusLog(TaskRunStatusLogPK):
    status: TaskRunStatus
    description: Optional[str] = None

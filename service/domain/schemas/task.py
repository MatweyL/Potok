from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field

from service.domain.schemas.enums import TaskType, TaskStatus, PriorityType


class TaskPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, TaskPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class TaskConfiguration(BaseModel):
    group_name: str
    priority: PriorityType = PriorityType.MEDIUM
    type: TaskType = TaskType.UNDEFINED
    monitoring_algorithm_id: int
    execution_arguments: Optional[Dict[str, Any]] = None


class Task(TaskPK, TaskConfiguration):
    status: TaskStatus
    status_updated_at: datetime
    payload_id: int = Field(description="Идентификатор полезной нагрузки задачи")


class TaskStatusLogPK(BaseModel):
    task_id: int
    status_updated_at: datetime

    def __eq__(self, other):
        return isinstance(other, TaskStatusLogPK) and self.task_id == other.task_id

    def __hash__(self):
        return hash(self.id)


class TaskStatusLog(TaskStatusLogPK):
    status: TaskStatus
    description: Optional[str] = None

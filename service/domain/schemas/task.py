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
    group_id: int = Field(description="Идентификатор группы")
    priority: PriorityType = Field(default=PriorityType.MEDIUM, description="Приоритет источника")
    type: TaskType = Field(default=TaskType.UNDEFINED, description="Тип задачи, определяющий способ учета прогресса")
    monitoring_algorithm_id: int= Field(description="Идентификатор алгоритма мониторинга")
    execution_arguments: Optional[Dict[str, Any]] = Field(default=None, description="Идентификатор группы")


class Task(TaskPK, TaskConfiguration):
    status: TaskStatus= Field(description="Статус выполнения задачи")
    status_updated_at: datetime= Field(description="Время обновления статуса")
    payload_id: int = Field(description="Идентификатор полезной нагрузки задачи")
    loaded_at: Optional[datetime] = Field(default=None, description="Дата загрузки задачи в хранилище")


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

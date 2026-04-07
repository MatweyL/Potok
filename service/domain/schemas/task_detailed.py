from typing import Optional

from pydantic import BaseModel

from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmUnion
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.domain.schemas.task_group import TaskGroup


class TaskDetailed(BaseModel):
    task: Task
    payload: Optional[Payload] = None
    monitoring_algorithm: Optional[MonitoringAlgorithmUnion] = None
    task_group: Optional[TaskGroup] = None
from typing import Optional, List, Dict

from pydantic import BaseModel

from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmUnion
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.domain.schemas.task_group import TaskGroup
from service.domain.schemas.task_progress import TaskProgress
from service.domain.schemas.task_run import TaskRun
from service.domain.schemas.task_run_metrics import StatusMetrics


class TaskDetailed(BaseModel):
    task: Task
    payload: Optional[Payload] = None
    monitoring_algorithm: Optional[MonitoringAlgorithmUnion] = None
    task_group: Optional[TaskGroup] = None  # TODO: refactor, remove task_ prefix
    task_runs_recent: Optional[List[TaskRun]] = None  # TODO: refactor, remove task_ prefix
    runs_status_metrics: Optional[StatusMetrics] = None
    progress: Optional[List[TaskProgress]] = None

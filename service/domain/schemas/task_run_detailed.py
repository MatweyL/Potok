from typing import List, Optional

from pydantic import BaseModel

from service.domain.schemas.task_run import TaskRun, TaskRunProgress


class TaskRunDetailed(BaseModel):
    task_run: TaskRun
    progress: Optional[List[TaskRunProgress]] = None

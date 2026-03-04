from typing import List, Optional

from pydantic import Field

from service.domain.schemas.enums import TaskType
from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_progress import TaskProgress, TimeIntervalTaskProgress, TimeIntervalTaskProgressPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class GetTaskProgressUCRq(UCRequest):
    task_id: int


class GetTaskProgressUCRs(UCResponse):
    request: GetTaskProgressUCRq
    task: Optional[Task] = None
    task_progress: List[TaskProgress] = Field(default_factory=list)


class GetTaskProgressUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK],
                 time_interval_task_progress_repo: Repo[
                     TimeIntervalTaskProgress, TimeIntervalTaskProgress, TimeIntervalTaskProgressPK]):
        self._task_repo = task_repo
        self._time_interval_task_progress_repo = time_interval_task_progress_repo

    async def apply(self, request: GetTaskProgressUCRq) -> GetTaskProgressUCRs:
        task = await self._task_repo.get(TaskPK(id=request.task_id))
        if not task:
            return GetTaskProgressUCRs(success=False, error="Not found", request=request)
        task_progress = None
        if task.type == TaskType.TIME_INTERVAL:
            task_progress = await self._time_interval_task_progress_repo.filter(
                FilterFieldsDNF.single('task_id', request.task_id))
        return GetTaskProgressUCRs(success=True, request=request, task=task, task_progress=task_progress)

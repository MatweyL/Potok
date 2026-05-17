from datetime import datetime
from typing import List, Dict

from service.domain.schemas.enums import TaskStatus
from service.domain.schemas.task import Task, TaskPK
from service.domain.use_cases.abstract import UCResponse, UCRequest, UseCase
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields, FilterFieldsDNF, FilterField, ConditionOperation

from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class ResumeTasksUCRq(UCRequest):
    tasks_ids: List[int]


class ResumeTasksUCRs(UCResponse):
    request: ResumeTasksUCRq
    resumed_task_by_id: Dict[int, Task] | None = None


class ResumeTasksUC(UseCase):

    def __init__(self, task_repo: Repo[Task, Task, TaskPK]):
        self._task_repo = task_repo

    async def apply(self, request: ResumeTasksUCRq) -> ResumeTasksUCRs:
        cancelled_tasks_condition = FilterFieldsDNF.single_conjunct(
            [
                FilterField(name='status', value=TaskStatus.CANCELLED),
                FilterField(name='id', value=request.tasks_ids, operation=ConditionOperation.IN)
            ]
        )
        status_updated_at = datetime.now()
        update_fields = UpdateFields.multiple({'status': TaskStatus.EXECUTION,
                                               'status_updated_at': status_updated_at})
        cancelled_tasks = await self._task_repo.filter(cancelled_tasks_condition)
        update_mapping = {cancelled_task: update_fields
                          for cancelled_task in cancelled_tasks}
        await self._task_repo.update_all(update_mapping)
        return ResumeTasksUCRs(success=True, request=request, resumed_task_by_id={t.id: t for t in cancelled_tasks})

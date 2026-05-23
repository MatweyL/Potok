from datetime import datetime, timezone
from typing import List, Dict

from service.domain.schemas.enums import TaskStatus
from service.domain.schemas.task import Task, TaskPK
from service.domain.use_cases.abstract import UCResponse, UCRequest, UseCase
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields, FilterFieldsDNF, FilterField, ConditionOperation


class CancelTasksUCRq(UCRequest):
    tasks_ids: List[int]


class CancelTasksUCRs(UCResponse):
    request: CancelTasksUCRq
    cancelled_task_by_id: Dict[int, Task] | None = None


class CancelTasksUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK]):
        self._task_repo = task_repo

    async def apply(self, request: CancelTasksUCRq) -> CancelTasksUCRs:
        status_updated_at = datetime.now(timezone.utc)
        update_fields = UpdateFields.multiple({'status': TaskStatus.CANCELLED,
                                               'status_updated_at': status_updated_at})
        await self._task_repo.update_all({TaskPK(id=task_id): update_fields
                                          for task_id in request.tasks_ids})
        cancelled_tasks_condition = FilterFieldsDNF.single_conjunct(
            [
                FilterField(name='status', value=TaskStatus.CANCELLED),
                FilterField(name='id', value=request.tasks_ids, operation=ConditionOperation.IN)
            ]
        )
        cancelled_tasks = await self._task_repo.filter(cancelled_tasks_condition)
        return CancelTasksUCRs(success=True, request=request, cancelled_task_by_id={t.id: t for t in cancelled_tasks})

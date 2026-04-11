from typing import Optional, Dict, Any

from service.domain.schemas.enums import PriorityType
from service.domain.schemas.task import Task, TaskPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields


class UpdateTaskUCRq(UCRequest):
    task_id: int
    priority: Optional[PriorityType] = None
    monitoring_algorithm_id: Optional[int] = None
    execution_arguments: Optional[Dict[str, Any]] = None


class UpdateTaskUCRs(UCResponse):
    request: UpdateTaskUCRq
    task: Optional[Task] = None


class UpdateTaskUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK]):
        self._task_repo = task_repo

    async def apply(self, request: UpdateTaskUCRq) -> UpdateTaskUCRs:
        existing = await self._task_repo.get(TaskPK(id=request.task_id))
        if not existing:
            return UpdateTaskUCRs(
                success=False, error="Task not found", request=request
            )

        updates = {}
        if request.priority is not None and request.priority != existing.priority:
            updates['priority'] = request.priority
        if request.monitoring_algorithm_id is not None and request.monitoring_algorithm_id != existing.monitoring_algorithm_id:
            updates['monitoring_algorithm_id'] = request.monitoring_algorithm_id
        if request.execution_arguments is not None:
            updates['execution_arguments'] = request.execution_arguments

        if not updates:
            return UpdateTaskUCRs(success=True, request=request, task=existing)

        updated = await self._task_repo.update(
            existing,
            UpdateFields.multiple(updates)
        )
        return UpdateTaskUCRs(success=True, request=request, task=updated)

from abc import ABC
from typing import Optional, List, Dict, Any

from service.adapters.outbound.repo.sa.impls.task_run import SATaskRunMetricsProvider
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.schemas.task_group import TaskGroupPK, TaskGroup, TaskGroupBody
from service.domain.schemas.task_run_metrics import TaskRunMetrics, TaskRunAvgMetrics, TaskRunGroupedMetrics, \
    TaskRunGroupedAvgMetrics
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUC, GetTasksDetailedUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, UpdateFields, PaginationQuery
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


class TaskGroupUC(UseCase, ABC):
    def __init__(self, task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK]):
        self._task_group_repo = task_group_repo


class CreateTaskGroupUCRq(UCRequest):
    task_group_body: TaskGroupBody


class CreateTaskGroupUCRs(UCResponse):
    request: CreateTaskGroupUCRq
    task_group: Optional[TaskGroup] = None


class CreateTaskGroupUC(TaskGroupUC):
    async def apply(self, request: CreateTaskGroupUCRq) -> CreateTaskGroupUCRs:
        existing_task_group = await self._task_group_repo.filter(
            FilterFieldsDNF.single('name', request.task_group_body.name)
        )
        if existing_task_group:
            return CreateTaskGroupUCRs(success=False,
                                       error=f"Task group with {request.task_group_body.name=} already exists!",
                                       request=request)
        task_group = TaskGroup.model_validate(request.task_group_body, from_attributes=True)
        task_group = await self._task_group_repo.create(task_group)
        return CreateTaskGroupUCRs(success=True, request=request, task_group=task_group)


class GetAllTaskGroupUCRq(UCRequest):
    filter_fields_dnf: Optional[FilterFieldsDNF] = None


class GetAllTaskGroupUCRs(UCResponse):
    request: GetAllTaskGroupUCRq
    task_groups: List[TaskGroup]


class GetAllTaskGroupUC(TaskGroupUC):

    async def apply(self, request: GetAllTaskGroupUCRq) -> GetAllTaskGroupUCRs:
        filter_fields_dnf = request.filter_fields_dnf if request.filter_fields_dnf else FilterFieldsDNF.empty()
        task_groups = await self._task_group_repo.filter(filter_fields_dnf)
        return GetAllTaskGroupUCRs(success=True, task_groups=task_groups, request=request, )


class GetTaskGroupUCRq(UCRequest):
    task_group_id: int


class GetTaskGroupUCRs(UCResponse):
    request: GetTaskGroupUCRq
    task_group: Optional[TaskGroup] = None
    task_run_grouped_metrics: Optional[TaskRunGroupedMetrics] = None
    task_run_grouped_avg_metrics: Optional[TaskRunGroupedAvgMetrics] = None


class GetTaskGroupUC(TaskGroupUC):
    async def apply(self, request: GetTaskGroupUCRq) -> GetTaskGroupUCRs:
        task_group = await self._task_group_repo.get(TaskGroupPK(id=request.task_group_id))
        if not task_group:
            return GetTaskGroupUCRs(success=False, request=request, error="Task group not found")

        return GetTaskGroupUCRs(success=True,
                                request=request,
                                task_group=task_group,
                                )


class UpdateTaskGroupUCRq(UCRequest):
    task_group_id: int

    # name намеренно отсутствует — менять нельзя,ломает очереди в брокере
    title: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    execution_arguments: Optional[Dict[str, Any]] = None
    queue_per_priority: Optional[bool] = None


class UpdateTaskGroupUCRs(UCResponse):
    request: UpdateTaskGroupUCRq
    task_group: Optional[TaskGroup] = None


class UpdateTaskGroupUC(TaskGroupUC):
    async def apply(self, request: UpdateTaskGroupUCRq) -> UpdateTaskGroupUCRs:
        existing = await self._task_group_repo.get(TaskGroupPK(id=request.task_group_id))
        if not existing:
            return UpdateTaskGroupUCRs(
                success=False, error="Task group not found", request=request
            )

        # Собираем только переданные поля
        updates: Dict[str, Any] = {}
        if request.title is not None:
            updates['title'] = request.title
        if request.description is not None:
            updates['description'] = request.description
        if request.is_active is not None:
            updates['is_active'] = request.is_active
        if request.execution_arguments is not None:
            updates['execution_arguments'] = request.execution_arguments
        if request.queue_per_priority is not None:
            updates['queue_per_priority'] = request.queue_per_priority

        if not updates:
            # Нечего обновлять — возвращаем как есть
            return UpdateTaskGroupUCRs(success=True, request=request, task_group=existing)

        updated = await self._task_group_repo.update(
            TaskGroupPK(id=request.task_group_id),
            UpdateFields.multiple(updates)
        )

        return UpdateTaskGroupUCRs(success=True, request=request, task_group=updated)


class GetTaskGroupDetailedTasksUCRq(UCRequest):
    task_group_id: int
    pagination: PaginationQuery


class GetTaskGroupDetailedTasksUCRs(UCResponse):
    request: GetTaskGroupDetailedTasksUCRq
    tasks_detailed: List[TaskDetailed]


class GetTaskGroupDetailedTasksUC(TaskGroupUC):
    def __init__(self, task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 get_tasks_detailed_uc: GetTasksDetailedUC, ):
        super().__init__(task_group_repo)
        self._get_tasks_detailed_uc = get_tasks_detailed_uc

    async def apply(self, request: GetTaskGroupDetailedTasksUCRq) -> GetTaskGroupDetailedTasksUCRs:
        filter_fields_dnf = FilterFieldsDNF.single('group_id', request.task_group_id)
        pagination = request.pagination
        pagination.filter_fields_dnf = filter_fields_dnf
        response = await self._get_tasks_detailed_uc.apply(GetTasksDetailedUCRq(pagination=pagination))
        return GetTaskGroupDetailedTasksUCRs(success=response.success, error=response.error, request=request,
                                             tasks_detailed=response.tasks)

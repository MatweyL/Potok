import pprint
from typing import List, Optional, Dict

from pydantic import Field

from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmUnion
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import TaskPK, Task
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_payload import GetPayloadUC, GetPayloadUCRq
from service.domain.use_cases.external.get_task_runs import GetTasksRunsUC, GetTasksRunsUCRq, GetTaskRunsUC, \
    GetTaskRunsUCRq
from service.domain.use_cases.external.get_tasks import GetTasksUC, GetTasksUCRq
from service.domain.use_cases.external.monitoring_algorithm import GetMonitoringAlgorithmUC, GetMonitoringAlgorithmUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


class GetTasksDetailedUCRq(UCRequest):
    pagination: PaginationQuery
    task_runs_recent_count: int = 3


class GetTasksDetailedUCRs(UCResponse):
    request: GetTasksDetailedUCRq
    tasks: List[TaskDetailed] = Field(default_factory=list)
    total: int
    total_filtered: int


class GetTasksDetailedUC(UseCase):
    def __init__(
            self,
            get_tasks_uc: GetTasksUC,
            get_task_runs_uc: GetTaskRunsUC,
            get_payload_uc: GetPayloadUC,
            get_monitoring_algorithm_uc: GetMonitoringAlgorithmUC,
            task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
            task_repo: Repo[Task, Task, TaskPK],
            task_run_metrics_provider: TaskRunMetricsProvider,
    ):
        self._get_tasks_uc = get_tasks_uc
        self._get_task_runs_uc = get_task_runs_uc
        self._get_payload_uc = get_payload_uc
        self._get_monitoring_algorithm_uc = get_monitoring_algorithm_uc
        self._task_group_repo = task_group_repo
        self._task_repo = task_repo
        self._task_run_metrics_provider = task_run_metrics_provider

    async def apply(self, request: GetTasksDetailedUCRq) -> GetTasksDetailedUCRs:
        tasks_rs = await self._get_tasks_uc.apply(GetTasksUCRq(pagination=request.pagination))
        if not tasks_rs.success:
            return GetTasksDetailedUCRs(success=False, error=tasks_rs.error, request=request, total=0, total_filtered=0)

        # Кеши на время одного запроса
        payload_cache: Dict[int, Optional[Payload]] = {}
        algorithm_cache: Dict[int, Optional[MonitoringAlgorithmUnion]] = {}
        task_group_cache: Dict[int, Optional[TaskGroup]] = {}
        result: List[TaskDetailed] = []
        task_detailed_by_id: Dict[int, TaskDetailed] = {}

        for task in tasks_rs.tasks:
            # Payload
            if task.payload_id not in payload_cache:
                payload_rs = await self._get_payload_uc.apply(
                    GetPayloadUCRq(payload_id=task.payload_id)
                )
                payload_cache[task.payload_id] = payload_rs.payload if payload_rs.success else None
            payload = payload_cache[task.payload_id]

            # Monitoring algorithm
            if task.monitoring_algorithm_id not in algorithm_cache:
                algorithm_rs = await self._get_monitoring_algorithm_uc.apply(
                    GetMonitoringAlgorithmUCRq(monitoring_algorithm_id=task.monitoring_algorithm_id)
                )
                algorithm_cache[task.monitoring_algorithm_id] = (
                    algorithm_rs.monitoring_algorithm if algorithm_rs.success else None
                )
            algorithm = algorithm_cache[task.monitoring_algorithm_id]

            if task.group_id not in task_group_cache:
                task_group = await self._task_group_repo.get(TaskGroupPK(id=task.group_id))
                task_group_cache[task.group_id] = task_group
            task_group = task_group_cache[task.group_id]
            task_detailed = TaskDetailed(task=task, payload=payload, monitoring_algorithm=algorithm,
                                         task_group=task_group)
            task_detailed_by_id[task_detailed.task.id] = task_detailed
            task_runs_rs = await self._get_task_runs_uc.apply(
                GetTaskRunsUCRq(
                    task_id=task.id,
                    pagination=PaginationQuery(
                        limit_per_page=request.task_runs_recent_count,
                        order_by="status_updated_at",
                        asc_sort=False
                    )
                )
            )
            task_detailed.task_runs_recent = task_runs_rs.task_runs
            result.append(task_detailed)

        tasks_ids = list(task_detailed_by_id.keys())
        tasks_runs_status_metrics = await self._task_run_metrics_provider.provide_tasks_runs_status_metrics(tasks_ids)
        for task_id, status_metrics in tasks_runs_status_metrics.status_metrics_by_task_id.items():
            task_detailed = task_detailed_by_id[task_id]
            task_detailed.task_runs_status_metrics = status_metrics
        total = await self._task_repo.count_by_fields(FilterFieldsDNF.empty())
        filter_fields_dnf = request.pagination.filter_fields_dnf if request.pagination.filter_fields_dnf else FilterFieldsDNF.empty()
        total_filtered = await self._task_repo.count_by_fields(filter_fields_dnf)
        return GetTasksDetailedUCRs(success=True, request=request, tasks=result, total=total,
                                    total_filtered=total_filtered)

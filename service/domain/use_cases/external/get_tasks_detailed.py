from typing import List, Optional, Dict

from pydantic import Field

from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmUnion
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import TaskPK, Task
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_payload import GetPayloadUC, GetPayloadUCRq
from service.domain.use_cases.external.get_tasks import GetTasksUC, GetTasksUCRq
from service.domain.use_cases.external.monitoring_algorithm import GetMonitoringAlgorithmUC, GetMonitoringAlgorithmUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF


class GetTasksDetailedUCRq(UCRequest):
    pagination: PaginationQuery


class GetTasksDetailedUCRs(UCResponse):
    request: GetTasksDetailedUCRq
    tasks: List[TaskDetailed] = Field(default_factory=list)
    total: int
    total_filtered: int


class GetTasksDetailedUC(UseCase):
    def __init__(
            self,
            get_tasks_uc: GetTasksUC,
            get_payload_uc: GetPayloadUC,
            get_monitoring_algorithm_uc: GetMonitoringAlgorithmUC,
            task_repo: Repo[Task, Task, TaskPK],
    ):
        self._get_tasks_uc = get_tasks_uc
        self._get_payload_uc = get_payload_uc
        self._get_monitoring_algorithm_uc = get_monitoring_algorithm_uc
        self._task_repo = task_repo

    async def apply(self, request: GetTasksDetailedUCRq) -> GetTasksDetailedUCRs:
        tasks_rs = await self._get_tasks_uc.apply(GetTasksUCRq(pagination=request.pagination))
        if not tasks_rs.success:
            return GetTasksDetailedUCRs(success=False, error=tasks_rs.error, request=request, total=0)

        # Кеши на время одного запроса
        payload_cache: Dict[int, Optional[Payload]] = {}
        algorithm_cache: Dict[int, Optional[MonitoringAlgorithmUnion]] = {}

        result: List[TaskDetailed] = []

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

            result.append(TaskDetailed(task=task, payload=payload, monitoring_algorithm=algorithm))
        total = await self._task_repo.count_by_fields(FilterFieldsDNF.empty())
        filter_fields_dnf = request.pagination.filter_fields_dnf if request.pagination.filter_fields_dnf else FilterFieldsDNF.empty()
        total_filtered = await self._task_repo.count_by_fields(filter_fields_dnf)
        return GetTasksDetailedUCRs(success=True, request=request, tasks=result, total=total,
                                    total_filtered=total_filtered)

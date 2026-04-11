from typing import Optional

from service.domain.schemas.payload import PayloadPK, Payload
from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUC, GetTaskProgressUCRq
from service.domain.use_cases.external.monitoring_algorithm import GetMonitoringAlgorithmUC, GetMonitoringAlgorithmUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


class GetTaskDetailedUCRq(UCRequest):
    task_id: int


class GetTaskDetailedUCRs(UCResponse):
    request: GetTaskDetailedUCRq
    task_detailed: Optional[TaskDetailed] = None


class GetTaskDetailedUC(UseCase):
    def __init__(self,
                 task_repo: Repo[Task, Task, TaskPK],
                 payload_repo: Repo[Payload, Payload, PayloadPK],
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 get_monitoring_algorithm_uc: GetMonitoringAlgorithmUC,
                 get_task_progress_uc: GetTaskProgressUC,
                 task_run_metrics_provider: TaskRunMetricsProvider,):
        self._task_repo = task_repo
        self._payload_repo = payload_repo
        self._task_group_repo = task_group_repo
        self._get_monitoring_algorithm_uc =get_monitoring_algorithm_uc
        self._get_task_progress_uc = get_task_progress_uc
        self._task_run_metrics_provider = task_run_metrics_provider

    async def apply(self, request: GetTaskDetailedUCRq) -> GetTaskDetailedUCRs:
        task = await self._task_repo.get(TaskPK(id=request.task_id))
        if not task:
            return GetTaskDetailedUCRs(success=False, error='Not found', request=request)
        payload = await self._payload_repo.get(PayloadPK(id=task.id))
        task_group = await self._task_group_repo.get(TaskGroupPK(id=task.group_id))
        monitoring_algorithm_rs = await self._get_monitoring_algorithm_uc.apply(
            GetMonitoringAlgorithmUCRq(monitoring_algorithm_id=task.monitoring_algorithm_id)
        )
        task_progress_rs = await self._get_task_progress_uc.apply(GetTaskProgressUCRq(task_id=task.id))
        task_run_status_metrics = await self._task_run_metrics_provider.provide_tasks_runs_status_metrics([task.id])

        task_detailed = TaskDetailed(task=task,
                                     payload=payload,
                                     monitoring_algorithm=monitoring_algorithm_rs.monitoring_algorithm,
                                     task_group=task_group,
                                     progress=task_progress_rs.task_progress,
                                     runs_status_metrics=task_run_status_metrics.status_metrics_by_task_id.get(task.id),
                                     )
        return GetTaskDetailedUCRs(success=True, request=request, task_detailed=task_detailed)

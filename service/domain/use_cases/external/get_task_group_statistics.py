from typing import Optional, List, Dict

from service.domain.schemas.task_group import TaskGroupStatistics, TaskGroupPK, TaskGroup
from service.domain.schemas.task_run_metrics import TaskRunAvgMetrics, TaskRunMetrics
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


def metrics_to_task_group_statistics(period_s: int,
                                     group_name: str,
                                     task_run_metrics: TaskRunMetrics,
                                     task_run_avg_metrics: TaskRunAvgMetrics, ) -> TaskGroupStatistics:
    metrics = task_run_metrics.grouped_metrics_by_name.get(group_name)
    avg_metrics = task_run_avg_metrics.grouped_avg_metrics_by_name.get(group_name)

    task_group_statistics = TaskGroupStatistics(group_name=group_name, period_s=period_s)
    if metrics:
        task_group_statistics.total_count = metrics.total
        task_group_statistics.waiting_count = metrics.waiting
        task_group_statistics.queued_count = metrics.queued
        task_group_statistics.execution_count = metrics.execution
        task_group_statistics.succeed_count = metrics.succeed
        task_group_statistics.error_count = metrics.error
        task_group_statistics.cancelled_count = metrics.cancelled
        task_group_statistics.interrupted_count = metrics.interrupted
        task_group_statistics.temp_error_count = metrics.temp_error
        task_group_statistics.completed_count = metrics.completed
        task_group_statistics.throughput = metrics.throughput
    if avg_metrics:
        task_group_statistics.avg_retry_count = avg_metrics.avg_retry_count
        task_group_statistics.avg_queued_duration = avg_metrics.avg_queued_duration
        task_group_statistics.avg_execution_duration = avg_metrics.avg_execution_duration
    return task_group_statistics


class GetTaskGroupStatisticsUCRq(UCRequest):
    task_group_id: int
    period_s: int = 86400


class GetTaskGroupStatisticsUCRs(UCResponse):
    request: GetTaskGroupStatisticsUCRq
    task_group_statistics: Optional[TaskGroupStatistics] = None


class GetTaskGroupStatisticsUC(UseCase):
    def __init__(self,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 task_run_metrics_provider: TaskRunMetricsProvider):
        self._task_group_repo = task_group_repo
        self._task_run_metrics_provider = task_run_metrics_provider

    async def apply(self, request: GetTaskGroupStatisticsUCRq) -> GetTaskGroupStatisticsUCRs:
        task_group = await self._task_group_repo.get(TaskGroupPK(id=request.task_group_id))
        if not task_group:
            return GetTaskGroupStatisticsUCRs(success=False, error="Task group not found", request=request)
        task_run_metrics = await self._task_run_metrics_provider.provide_by_period(request.period_s, task_group.name)
        task_run_avg_metrics = await self._task_run_metrics_provider.provide_avg_by_period(request.period_s,
                                                                                           task_group.name)
        task_group_statistics = metrics_to_task_group_statistics(request.period_s, task_group.name,
                                                                 task_run_metrics, task_run_avg_metrics, )
        return GetTaskGroupStatisticsUCRs(success=True, request=request, task_group_statistics=task_group_statistics)


class GetAllTaskGroupStatisticsUCRq(UCRequest):
    task_group_ids: Optional[List[int]] = None
    period_s: int = 86400


class GetAllTaskGroupStatisticsUCRs(UCResponse):
    request: GetAllTaskGroupStatisticsUCRq
    task_group_statistics_by_name: Optional[Dict[str, TaskGroupStatistics]] = None


class GetAllTaskGroupStatisticsUC(UseCase):
    def __init__(self,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 task_run_metrics_provider: TaskRunMetricsProvider):
        self._task_group_repo = task_group_repo
        self._task_run_metrics_provider = task_run_metrics_provider

    async def apply(self, request: GetAllTaskGroupStatisticsUCRq) -> GetAllTaskGroupStatisticsUCRs:
        if not request.task_group_ids:
            task_run_metrics = await self._task_run_metrics_provider.provide_by_period(request.period_s, )
            task_run_avg_metrics = await self._task_run_metrics_provider.provide_avg_by_period(request.period_s, )
        else:
            filter_fields_dnf = FilterFieldsDNF.single('id', request.task_group_ids, ConditionOperation.IN)
            task_groups = await self._task_group_repo.filter(filter_fields_dnf)
            group_names = [task_group.name for task_group in task_groups]
            if group_names:
                task_run_metrics = await self._task_run_metrics_provider.provide_by_period(request.period_s,
                                                                                           group_names)
                task_run_avg_metrics = await self._task_run_metrics_provider.provide_avg_by_period(request.period_s,
                                                                                                   group_names)
            else:
                task_run_metrics = TaskRunMetrics(grouped_metrics_by_name={})
                task_run_avg_metrics = TaskRunAvgMetrics(grouped_avg_metrics_by_name={})
        task_group_statistics_by_name = {}
        group_names = set(task_run_metrics.grouped_metrics_by_name.keys())
        group_names.update(task_run_avg_metrics.grouped_avg_metrics_by_name.keys())
        for group_name in group_names:
            task_group_statistics_by_name[group_name] = metrics_to_task_group_statistics(request.period_s,
                                                                                         group_name,
                                                                                         task_run_metrics,
                                                                                         task_run_avg_metrics)

        return GetAllTaskGroupStatisticsUCRs(success=True, request=request,
                                             task_group_statistics_by_name=task_group_statistics_by_name)

from typing import Dict

from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


def clip(value, value_min=None, value_max=None, ) -> int | float:
    if not value_min and not value_max:
        return value
    if value_max and value > value_max:
        return value_max
    if value_min and value < value_min:
        return value_min
    return value


class AIMDBalancingAlgorithm(BalancingAlgorithm):
    def __init__(self, task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 task_run_metrics_provider: TaskRunMetricsProvider,
                 batch_size_min: int,
                 batch_size_max: int,
                 delta: float,
                 beta: float,
                 period_s: int = 600,
                 ):
        super().__init__(task_group_repo)
        self._task_run_metrics_provider = task_run_metrics_provider
        self._period_s = period_s
        self._batch_size_min = batch_size_min
        self._batch_size_max = batch_size_max
        self._delta = delta
        self._beta = beta
        self._batch_size_by_group = {}

    async def calculate_batch_size_by_group(self) -> Dict[str, int]:
        task_run_metrics = await self._task_run_metrics_provider.provide_by_period(self._period_s)
        batch_size_by_group = {}
        for group_name, task_run_grouped_metrics in task_run_metrics.grouped_metrics_by_name.items():
            succeed = task_run_grouped_metrics.completed
            error = task_run_grouped_metrics.failed
            total = error + succeed
            succeed_frequency_by_period = succeed / total if total else 0

            current_batch_size = self._batch_size_by_group.get(group_name, self._batch_size_min)
            if succeed_frequency_by_period >= 0.85:
                current_batch_size += self._delta
            elif succeed_frequency_by_period < 0.7:
                current_batch_size *= self._beta
            current_batch_size = clip(current_batch_size, self._batch_size_min, self._batch_size_max)
            batch_size_by_group[group_name] = current_batch_size
            self._batch_size_by_group[group_name] = current_batch_size
        logger.info(f"AIMD batch sizes: {batch_size_by_group}")
        return batch_size_by_group

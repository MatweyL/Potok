from typing import Iterator

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskRunStatusLog, SystemTime, TaskBatchProviderType, TaskRunStatus
from imitation_modelling.task_batch_provider import TaskBatchProvider
from service.ports.common.logs import logger


def clip(value, value_min=None, value_max=None, ) -> int | float:
    if not value_min and not value_max:
        return value
    if value_max and value > value_max:
        return value_max
    if value_min and value < value_min:
        return value_min
    return value

class AIMDTaskBatchProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.AIMD

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 delta: int, beta: float,
                 base_batch_size: int,
                 batch_size_min: int,
                 batch_size_max: int, ):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time, )
        self.delta = delta
        self.beta = beta
        self.current_batch_size = base_batch_size
        self.batch_size_min = batch_size_min
        self.batch_size_max = batch_size_max

    def iter(self) -> Iterator[TaskRunStatusLog]:
        succeed = self._task_run_metric_provider.get_succeed_by_period()
        error = self._task_run_metric_provider.get_error_by_period()
        total = error + succeed
        succeed_frequency_by_period = succeed / total if total else 0
        if succeed_frequency_by_period >= 0.85:
            self.current_batch_size += self.delta
        elif succeed_frequency_by_period < 0.7:
            self.current_batch_size *= self.beta
        self.current_batch_size = clip(self.current_batch_size, self.batch_size_min, self.batch_size_max)
        logger.info(f"{self.current_batch_size=}")
        tasks_count = 0
        current_batch_size = self.current_batch_size
        for actual_task_run_status_log in self._task_run_status_repo.iter_actual_statuses({TaskRunStatus.WAITING}):
            yield actual_task_run_status_log
            tasks_count += 1
            if tasks_count >= current_batch_size:
                break

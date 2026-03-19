from abc import abstractmethod, ABC
from typing import List, Iterator

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunMetricProvider, TaskRunStatusRepo
from imitation_modelling.schemas import SystemTime, TaskRunStatusLog, TaskRunStatus, TaskBatchProviderType


class TaskBatchProvider(ABC):
    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime, **kwargs):
        self._broker = broker
        self._task_run_status_repo = task_run_status_repo
        self._task_run_metric_provider = task_run_metric_provider
        self._system_time = system_time

    def get(self) -> List[TaskRunStatusLog]:
        return list(self.iter())

    @abstractmethod
    def calculate_batch_size(self) -> int:
        pass

    def iter(self) -> Iterator[TaskRunStatusLog]:
        batch_size = self.calculate_batch_size()
        tasks_count = 0
        for actual_task_run_status_log in self._task_run_status_repo.iter_actual_statuses({TaskRunStatus.WAITING}):
            yield actual_task_run_status_log
            tasks_count += 1
            if tasks_count >= batch_size:
                break

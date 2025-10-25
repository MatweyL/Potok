from abc import abstractmethod, ABC
from typing import List, Iterator, Dict, Any, Type

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
    def iter(self) -> Iterator[TaskRunStatusLog]:
        pass


class ConstantSizeTaskBatchProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.CONSTANT_SIZE

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 batch_size: int, ):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._batch_size = batch_size

    def iter(self) -> Iterator[TaskRunStatusLog]:
        tasks_count = 0
        for actual_task_run_status_log in self._task_run_status_repo.iter_actual_statuses({TaskRunStatus.WAITING}):
            yield actual_task_run_status_log
            tasks_count += 1
            if tasks_count >= self._batch_size:
                break


class TaskBatchProviderBuilder:
    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime, ):
        self._broker = broker
        self._task_run_status_repo = task_run_status_repo
        self._task_run_metric_provider = task_run_metric_provider
        self._system_time = system_time

        self._class_by_type: Dict[TaskBatchProviderType, Type[TaskBatchProvider]] = {
            TaskBatchProviderType.CONSTANT_SIZE: ConstantSizeTaskBatchProvider,
        }

    def build(self, task_batch_provider_type: TaskBatchProviderType, params: Dict[str, Any]):
        cls = self._class_by_type.get(task_batch_provider_type)
        if not task_batch_provider_type:
            raise RuntimeError(f"Unexpected task batch provider type: {task_batch_provider_type}")
        return cls(self._broker, self._task_run_status_repo,self._task_run_metric_provider, self._system_time, **params)
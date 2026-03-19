from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunMetricProvider, TaskRunStatusRepo
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class ConstantSizeTaskBatchProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.CONSTANT_SIZE

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 batch_size: int, ):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._batch_size = batch_size

    def calculate_batch_size(self) -> int:
        return self._batch_size

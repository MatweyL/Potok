from typing import Dict, Any

from imitation_modelling.broker import Broker
from imitation_modelling.metric_collector import MetricCollector
from imitation_modelling.repo import TaskRunMetricProvider, TaskRunStatusRepo
from imitation_modelling.schemas import SystemTime, TaskBatchProviderType
from imitation_modelling.task_batch_provider import ConstantSizeTaskBatchProvider
from imitation_modelling.task_batch_provider_aimd import AIMDTaskBatchProvider
from imitation_modelling.task_batch_provider_pid import PIDTaskBatchProvider


class TaskBatchProviderBuilder:
    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 metric_collector: MetricCollector, ):
        self._broker = broker
        self._task_run_status_repo = task_run_status_repo
        self._task_run_metric_provider = task_run_metric_provider
        self._system_time = system_time
        self._metric_collector = metric_collector

    def build(self, task_batch_provider_type: TaskBatchProviderType, params: Dict[str, Any]):
        if not task_batch_provider_type:
            raise RuntimeError(f"Unexpected task batch provider type: {task_batch_provider_type}")
        if task_batch_provider_type == TaskBatchProviderType.CONSTANT_SIZE:
            return ConstantSizeTaskBatchProvider(self._broker, self._task_run_status_repo,
                                                 self._task_run_metric_provider, self._system_time,
                                                 **params)
        if task_batch_provider_type == TaskBatchProviderType.AIMD:
            return AIMDTaskBatchProvider(self._broker, self._task_run_status_repo, self._task_run_metric_provider,
                                         self._system_time,
                                         **params)
        return PIDTaskBatchProvider(self._broker, self._task_run_status_repo, self._task_run_metric_provider,
                                    self._system_time,
                                    self._metric_collector,
                                    **params)

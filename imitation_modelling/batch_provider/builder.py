from typing import Dict, Any

from imitation_modelling.batch_provider.adaptive_model import AdaptiveModelProvider
from imitation_modelling.batch_provider.aimd import AIMDTaskBatchProvider
from imitation_modelling.batch_provider.constant import ConstantSizeTaskBatchProvider
from imitation_modelling.batch_provider.gradient_ascent import GradientAscentProvider
from imitation_modelling.batch_provider.moving_pid import MovingPIDProvider
from imitation_modelling.batch_provider.moving_pid_v2 import MovingPIDV2Provider
from imitation_modelling.broker import Broker
from imitation_modelling.metric_collector import MetricCollector
from imitation_modelling.repo import TaskRunMetricProvider, TaskRunStatusRepo
from imitation_modelling.schemas import SystemTime, TaskBatchProviderType


class TaskBatchProviderBuilder:
    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 metric_collector: MetricCollector, ):
        self._broker = broker
        self._task_run_status_repo = task_run_status_repo
        self._task_run_metric_provider = task_run_metric_provider
        self._system_time = system_time
        self._metric_collector = metric_collector
        self._algo_cls_by_type = {TaskBatchProviderType.MOVING_PID: MovingPIDProvider,
                                  TaskBatchProviderType.AIMD: AIMDTaskBatchProvider,
                                  TaskBatchProviderType.CONSTANT_SIZE: ConstantSizeTaskBatchProvider,
                                  TaskBatchProviderType.MOVING_PID_V2: MovingPIDV2Provider,
                                  TaskBatchProviderType.ADAPTIVE_MODEL: AdaptiveModelProvider,
                                  TaskBatchProviderType.GRADIENT_ASCENT: GradientAscentProvider,
                                  }

    def build(self, task_batch_provider_type: TaskBatchProviderType, params: Dict[str, Any]):
        if not task_batch_provider_type:
            raise RuntimeError(f"Unexpected task batch provider type: {task_batch_provider_type}")
        algo_cls = self._algo_cls_by_type.get(task_batch_provider_type)
        if not algo_cls:
            raise RuntimeError("unknown type")
        return algo_cls(self._broker,
                        self._task_run_status_repo,
                        self._task_run_metric_provider,
                        self._system_time,
                        **params)

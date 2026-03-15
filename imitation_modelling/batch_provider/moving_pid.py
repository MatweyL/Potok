import enum
from time import perf_counter
from typing import Iterator

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, TaskRunStatusLog, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class MovingPIDState(str, enum.Enum):
    COLD_START = "COLD_START"
    RANGE_RETENTION = "RANGE_RETENTION"
    ADJUSTMENT = "ADJUSTMENT"


class MovingPIDProvider(TaskBatchProvider):
    type: TaskBatchProviderType.MOVING_PID

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._state = MovingPIDState.COLD_START
        self._cold_start_batch_size = 0

        self._last_call_at = 0
        self._calls_count = 0

    def iter(self) -> Iterator[TaskRunStatusLog]:
        # Этап холодного старта. Главная проблема - мы ничего не знаем о производительности внешней системы,
        # но нам максимально быстро нужно узнать о ней
        if self._state == MovingPIDState.COLD_START:
            pass
        elif self._state == MovingPIDState.RANGE_RETENTION:
            pass
        elif self._state == MovingPIDState.ADJUSTMENT:
            pass
        else:
            raise RuntimeError(f"Unknown state: {self._state}")

        self._calls_count += 1
        self._last_call_at = perf_counter()

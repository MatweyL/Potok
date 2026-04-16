import enum
import math

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class GradientAscentState(str, enum.Enum):
    COLD_START = "COLD_START"
    GRADIENT_ASCENT = "GRADIENT_ASCENT"


class GradientAscentProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.GRADIENT_ASCENT

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 2.0,
                 learning_rate: float = 2.0,  # Уменьшен для стабильности
                 gradient_ema_alpha: float = 0.3,
                 max_step_fraction: float = 0.2,  # Более консервативно
                 min_exploration_step: int = 1,
                 overload_shrink_factor: float = 0.7,
                 batch_size_min: int = 1):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._state = GradientAscentState.COLD_START

        # Параметры
        self._cold_start_growth_multiplier = cold_start_growth_multiplier
        self._learning_rate = learning_rate
        self._gradient_ema_alpha = gradient_ema_alpha
        self._max_step = max_step_fraction
        self._min_step = min_exploration_step
        self._shrink = overload_shrink_factor
        self._batch_size_min = batch_size_min

        # Состояние
        self._batch_size: float = float(cold_start_batch_size)
        self._ema_throughput: float = 0.0
        self._ema_gradient: float = 0.0

        # История для расчета дельты
        self._prev_batch: float = 0.0
        self._prev_throughput: float = 0.0
        self._last_completed_count: int = 0
        self._calls_count: int = 0

    def calculate_batch_size(self) -> int:
        metrics = self._task_run_metric_provider
        completed = metrics.get_completed_count()

        # 1. Считаем текущий throughput
        current_throughput = completed - self._last_completed_count
        self._ema_throughput = (self._gradient_ema_alpha * current_throughput +
                                (1 - self._gradient_ema_alpha) * self._ema_throughput)

        overload = metrics.get_temp_error_count_total() + metrics.get_interrupted_count_total()
        in_flight = metrics.get_execution_count_total() + metrics.get_queued_count_total()

        self._calls_count += 1

        # 2. Логика переключения состояний
        if self._state == GradientAscentState.COLD_START:
            if self._calls_count == 1:
                self._batch_size = self._batch_size or max(1.0, metrics.get_total_count() // 10)
            elif overload > 0 or in_flight > self._ema_throughput * 3:
                self._batch_size = max(self._batch_size_min, self._batch_size * self._shrink)
                self._state = GradientAscentState.GRADIENT_ASCENT
            else:
                self._batch_size *= self._cold_start_growth_multiplier

        # 3. Основной цикл градиента
        elif self._state == GradientAscentState.GRADIENT_ASCENT:
            if overload > 0:
                # Реакция на перегрузку (как в Adaptive Model)
                self._batch_size = max(self._batch_size_min, self._batch_size * self._shrink)
                self._ema_gradient = 0  # Сбрасываем инерцию
            else:
                # Считаем градиент: (dT / dB)
                d_batch = self._batch_size - self._prev_batch
                d_tp = self._ema_throughput - self._prev_throughput

                if abs(d_batch) > 0.1:
                    raw_grad = d_tp / d_batch
                    self._ema_gradient = (self._gradient_ema_alpha * raw_grad +
                                          (1 - self._gradient_ema_alpha) * self._ema_gradient)

                # Вычисляем шаг
                step = self._ema_gradient * self._learning_rate

                # Ограничиваем шаг (Clamp)
                limit = self._batch_size * self._max_step
                step = max(-limit, min(limit, step))

                # Принудительное исследование (Exploration), если застряли
                if abs(step) < self._min_step:
                    step = self._min_step if self._ema_gradient >= 0 else -self._min_step

                self._batch_size = max(self._batch_size_min, self._batch_size + step)

        # 4. Сохраняем историю для следующего шага
        self._prev_batch = self._batch_size
        self._prev_throughput = self._ema_throughput
        self._last_completed_count = completed

        return int(self._batch_size)
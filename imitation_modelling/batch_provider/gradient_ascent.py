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
    type: TaskBatchProviderType = TaskBatchProviderType.GRADIENT_ASCENT  # fix 1

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 2.0,
                 learning_rate: float = 5.0,
                 gradient_ema_alpha: float = 0.3,
                 max_step_fraction: float = 0.3,       # fix 7: относительный, не абсолютный
                 min_exploration_step: int = 1,         # fix 8: минимальный шаг исследования
                 overload_shrink_factor: float = 0.7,  # fix 3: вынесено из магии
                 batch_size_min: int = 1):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._state = GradientAscentState.COLD_START
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier
        self._learning_rate = learning_rate
        self._gradient_ema_alpha = gradient_ema_alpha
        self._max_step_fraction = max_step_fraction
        self._min_exploration_step = min_exploration_step
        self._overload_shrink_factor = overload_shrink_factor
        self._batch_size_min = batch_size_min

        self._batch_size: float = 0.0
        self._batch_size_remainder: float = 0.0  # fix 2: накапливаем дробный остаток
        self._calls_count: int = 0

        self._last_completed_count: int = 0
        self._last_throughput: float = 0.0
        self._last_batch_size: float = 0.0

        self._ema_throughput: float = 0.0
        self._ema_gradient: float = 0.0
        self._gradient_known: bool = False  # fix 6: явный флаг наличия валидного градиента

    def _update_throughput(self, completed_count: int) -> float:
        """Считает сырой throughput и обновляет EMA. Вызывается в начале итерации."""  # fix 4
        throughput = completed_count - self._last_completed_count
        self._ema_throughput = (self._gradient_ema_alpha * throughput
                                + (1 - self._gradient_ema_alpha) * self._ema_throughput)
        return throughput

    def _apply_step(self, step: float) -> None:
        """Двигает batch_size на step с учётом дробного остатка и ограничений."""  # fix 2
        raw = self._batch_size + step + self._batch_size_remainder
        floored = math.floor(raw)
        self._batch_size_remainder = raw - floored
        self._batch_size = max(float(self._batch_size_min), float(floored))

    def calculate_batch_size(self) -> int:
        completed_count   = self._task_run_metric_provider.get_completed_count()
        temp_error_count  = self._task_run_metric_provider.get_temp_error_count_total()
        interrupted_count = self._task_run_metric_provider.get_interrupted_count_total()
        execution_count   = self._task_run_metric_provider.get_execution_count_total()
        queued_count      = self._task_run_metric_provider.get_queued_count_total()
        waiting_count     = self._task_run_metric_provider.get_waiting_count_total()

        # fix 4: обновление метрик изолировано в методе, порядок очевиден
        self._update_throughput(completed_count)

        overload  = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count
        in_tail   = waiting_count < max(self._batch_size, 1)

        self._calls_count += 1

        # ── Cold start ────────────────────────────────────────────────────────
        if self._state == GradientAscentState.COLD_START:
            if self._calls_count == 1:
                if not self._cold_start_batch_size:
                    self._batch_size = max(1.0, self._task_run_metric_provider.get_total_count() // 10)
                else:
                    self._batch_size = float(self._cold_start_batch_size)

                # fix 5: инициализируем историю уже на первой итерации,
                # чтобы первый градиент после cold start был корректным
                self._last_batch_size = self._batch_size
                self._last_throughput = self._ema_throughput

            else:
                if overload > 0 or in_flight > self._ema_throughput * 3:
                    # Откатываемся на предыдущее (чистое) значение батча
                    self._batch_size = max(
                        self._batch_size_min,
                        self._batch_size / self._cold_start_growth_multiplier,
                    )
                    # История уже актуальна с предыдущей итерации — градиент
                    # на первом шаге GRADIENT_ASCENT будет считаться корректно
                    self._state = GradientAscentState.GRADIENT_ASCENT
                else:
                    self._last_batch_size = self._batch_size
                    self._last_throughput = self._ema_throughput
                    self._batch_size *= self._cold_start_growth_multiplier

        # ── Gradient ascent ───────────────────────────────────────────────────
        elif self._state == GradientAscentState.GRADIENT_ASCENT:

            if overload > 0:
                # Перегрузка обрабатывается независимо от градиента  # fix 3
                new_batch = self._batch_size * self._overload_shrink_factor
                self._last_batch_size = self._batch_size
                self._last_throughput = self._ema_throughput
                self._batch_size = max(float(self._batch_size_min), new_batch)
                self._batch_size_remainder = 0.0  # сброс остатка при резком снижении

            elif not in_tail:
                d_batch = self._batch_size - self._last_batch_size
                d_throughput = self._ema_throughput - self._last_throughput

                if abs(d_batch) > 1e-6:
                    # fix 6: градиент валиден только если батч реально менялся
                    raw_gradient = d_throughput / d_batch
                    self._ema_gradient = (self._gradient_ema_alpha * raw_gradient
                                          + (1 - self._gradient_ema_alpha) * self._ema_gradient)
                    self._gradient_known = True
                # else: батч не менялся — ema_gradient не трогаем, шаг всё равно делаем ниже

                self._last_batch_size = self._batch_size
                self._last_throughput = self._ema_throughput

                if self._gradient_known:
                    step = self._learning_rate * self._ema_gradient
                    # fix 7: ограничение относительное
                    max_step = self._max_step_fraction * self._batch_size
                    step = max(-max_step, min(max_step, step))
                else:
                    step = 0.0

                # fix 8: минимальный шаг исследования — алгоритм не замирает
                if abs(step) < self._min_exploration_step:
                    direction = math.copysign(1.0, self._ema_gradient) if self._ema_gradient != 0 else 1.0
                    step = direction * self._min_exploration_step

                self._apply_step(step)

        else:
            raise RuntimeError(f"Unknown state: {self._state}")

        # fix 4: обновление completed_count явно в конце, после всех вычислений
        self._last_completed_count = completed_count
        return int(self._batch_size)

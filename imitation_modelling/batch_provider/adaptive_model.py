import enum
import math

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class AdaptiveModelState(str, enum.Enum):
    COLD_START = "COLD_START"
    EXPLOITATION = "EXPLOITATION"  # идём в B_peak
    EXPLORATION = "EXPLORATION"  # пробуем соседа для обновления модели


class AdaptiveModelProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.ADAPTIVE_MODEL

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 2.0,
                 model_alpha_low: float = 0.2,
                 model_alpha_peak: float = 0.3,
                 model_alpha_high: float = 0.5,
                 throughput_ema_alpha: float = 0.3,
                 exploration_interval: int = 5,
                 exploration_step_fraction: float = 0.15,
                 min_model_width: int = 4,
                 overload_rate_threshold: float = 0.1):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)

        self._state = AdaptiveModelState.COLD_START
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier

        self._model_alpha_low = model_alpha_low
        self._model_alpha_peak = model_alpha_peak
        self._model_alpha_high = model_alpha_high
        self._throughput_ema_alpha = throughput_ema_alpha

        self._exploration_interval = exploration_interval
        self._exploration_step_fraction = exploration_step_fraction
        self._min_model_width = min_model_width
        self._overload_rate_threshold = overload_rate_threshold

        # Опорные точки модели
        self._b_low: float = 0.0
        self._b_peak: float = 0.0
        self._b_high: float = 0.0
        self._t_low: float = 0.0
        self._t_peak: float = 0.0
        self._t_high: float = 0.0

        self._batch_size: float = 0.0
        self._ema_throughput: float = 0.0
        self._last_completed_count: int = 0
        self._calls_count: int = 0
        self._exploitation_calls: int = 0
        self._exploration_direction: int = 1
        self._exploration_steps_done: int = 0

        # --- PID Инициализация ---
        # Коэффициенты подобраны для стабильности в задачах пакетной обработки
        self._kp = 0.4  # Реакция на отклонение от цели
        self._ki = 0.05  # Устранение статического отставания
        self._kd = 0.5  # Демпфирование при росте очереди (in_flight)

        self._integral_error: float = 0.0
        self._last_pid_error: float = 0.0
        self._last_in_flight: int = 0

    # ── Вспомогательные методы ────────────────────────────────────────────────

    def _update_throughput_ema(self, completed_count: int) -> None:
        throughput = completed_count - self._last_completed_count
        self._ema_throughput = (self._throughput_ema_alpha * throughput
                                + (1 - self._throughput_ema_alpha) * self._ema_throughput)

    def _is_overloaded(self, overload: int, in_flight: int) -> bool:
        if in_flight == 0:
            return False
        return (overload / in_flight) > self._overload_rate_threshold

    def _apply_pid(self, target_batch: float, current_in_flight: int) -> float:
        """
        Вычисляет корректировку батча.
        Вместо классического отклонения по времени, D-компонента гасит рост очереди.
        """
        error = target_batch - self._batch_size

        # P: Пропорциональный шаг к цели
        p_term = self._kp * error

        # I: Накопление (с ограничением, чтобы не "раздувало" батч вечно)
        self._integral_error = max(-20.0, min(20.0, self._integral_error + error))
        i_term = self._ki * self._integral_error

        # D: Реакция на изменение забитости системы.
        # Если in_flight вырос, d_term > 0, и мы ВЫЧИТАЕМ его из результата (тормозим)
        in_flight_delta = current_in_flight - self._last_in_flight
        d_term = self._kd * in_flight_delta

        self._last_pid_error = error
        self._last_in_flight = current_in_flight

        return p_term + i_term - d_term

    def _assign_observation_to_zone(self, batch: float, throughput: float) -> None:
        mid_low = (self._b_low + self._b_peak) / 2
        mid_high = (self._b_peak + self._b_high) / 2

        if batch <= mid_low:
            self._b_low = (1 - self._model_alpha_low) * self._b_low + self._model_alpha_low * batch
            self._t_low = (1 - self._model_alpha_low) * self._t_low + self._model_alpha_low * throughput
        elif batch >= mid_high:
            self._b_high = (1 - self._model_alpha_high) * self._b_high + self._model_alpha_high * batch
            self._t_high = (1 - self._model_alpha_high) * self._t_high + self._model_alpha_high * throughput
        else:
            self._b_peak = (1 - self._model_alpha_peak) * self._b_peak + self._model_alpha_peak * batch
            self._t_peak = (1 - self._model_alpha_peak) * self._t_peak + self._model_alpha_peak * throughput
        self._enforce_model_consistency()

    def _enforce_model_consistency(self) -> None:
        half = self._min_model_width / 2
        self._b_low = min(self._b_low, self._b_peak - half)
        self._b_high = max(self._b_high, self._b_peak + half)
        self._b_low = max(1.0, self._b_low)

    def _predict_best_batch(self) -> float:
        if self._t_peak >= self._t_low and self._t_peak >= self._t_high:
            return self._b_peak
        elif self._t_low > self._t_peak:
            return (self._b_peak + self._b_high) / 2
        else:
            return self._b_peak

    def _exploration_step_size(self) -> float:
        return max(1.0, self._exploration_step_fraction * (self._b_high - self._b_low))

    # ── Основной метод ────────────────────────────────────────────────────────

    def calculate_batch_size(self) -> int:
        completed_count = self._task_run_metric_provider.get_completed_count()
        temp_error_count = self._task_run_metric_provider.get_temp_error_count_total()
        interrupted_count = self._task_run_metric_provider.get_interrupted_count_total()
        execution_count = self._task_run_metric_provider.get_execution_count_total()
        queued_count = self._task_run_metric_provider.get_queued_count_total()
        waiting_count = self._task_run_metric_provider.get_waiting_count_total()

        self._update_throughput_ema(completed_count)

        overload = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count
        in_tail = waiting_count < max(self._batch_size, 1)
        is_overloaded = self._is_overloaded(overload, in_flight)

        self._calls_count += 1
        target_batch = self._batch_size  # По умолчанию

        # ── Cold start ────────────────────────────────────────────────────────
        if self._state == AdaptiveModelState.COLD_START:
            if self._calls_count == 1:
                self._batch_size = float(self._cold_start_batch_size) if self._cold_start_batch_size else max(1.0,
                                                                                                              self._task_run_metric_provider.get_total_count() // 10)
            else:
                if is_overloaded or in_flight > self._ema_throughput * 3:
                    self._b_high = self._batch_size
                    self._b_low = max(1.0, self._batch_size / 4)
                    self._b_peak = (self._b_low + self._b_high) / 2
                    self._t_low, self._t_peak, self._t_high = self._ema_throughput * 0.5, self._ema_throughput, self._ema_throughput * 0.5
                    self._state = AdaptiveModelState.EXPLOITATION
                    target_batch = self._b_peak
                else:
                    self._batch_size *= self._cold_start_growth_multiplier
                    target_batch = self._batch_size

        # ── Exploitation ──────────────────────────────────────────────────────
        elif self._state == AdaptiveModelState.EXPLOITATION:
            if not in_tail:
                self._assign_observation_to_zone(self._batch_size, self._ema_throughput)
                if is_overloaded:
                    self._b_high = max(self._b_peak + self._min_model_width, self._batch_size * 0.85)
                    self._enforce_model_consistency()
                    self._integral_error *= 0.5  # Частичный сброс при аварии

                self._exploitation_calls += 1
                if self._exploitation_calls >= self._exploration_interval:
                    self._exploration_direction = 1 if (
                                                                   self._exploitation_calls // self._exploration_interval) % 2 == 1 else -1
                    self._state = AdaptiveModelState.EXPLORATION
                    target_batch = self._predict_best_batch() + (
                                self._exploration_direction * self._exploration_step_size())
                else:
                    target_batch = self._predict_best_batch()

        # ── Exploration ───────────────────────────────────────────────────────
        elif self._state == AdaptiveModelState.EXPLORATION:
            if not in_tail:
                self._assign_observation_to_zone(self._batch_size, self._ema_throughput)
                if is_overloaded:
                    self._b_high = max(self._b_peak + self._min_model_width, self._batch_size * 0.85)
                    self._state = AdaptiveModelState.EXPLOITATION
                    target_batch = self._predict_best_batch()
                else:
                    self._exploration_steps_done += 1
                    if self._exploration_steps_done < 2:
                        target_batch = self._batch_size + self._exploration_direction * self._exploration_step_size()
                    else:
                        self._state = AdaptiveModelState.EXPLOITATION
                        target_batch = self._predict_best_batch()
                        self._exploration_steps_done = 0

        # --- Применение PID коррекции (кроме Cold Start) ---
        if self._state != AdaptiveModelState.COLD_START:
            # PID вычисляет шаг к цели, учитывая текущий in_flight
            pid_adjustment = self._apply_pid(target_batch, in_flight)
            self._batch_size += pid_adjustment

        self._last_completed_count = completed_count
        self._batch_size = max(1.0, min(self._batch_size, self._b_high))
        return int(self._batch_size)
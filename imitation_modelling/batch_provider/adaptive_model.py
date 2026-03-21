import enum
import math

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class AdaptiveModelState(str, enum.Enum):
    COLD_START = "COLD_START"
    EXPLOITATION = "EXPLOITATION"   # идём в B_peak
    EXPLORATION = "EXPLORATION"     # пробуем соседа для обновления модели


class AdaptiveModelProvider(TaskBatchProvider):
    """
    Предиктивный алгоритм с онлайн-моделью throughput = f(batch_size).

    Модель — кусочно-линейная с тремя опорными точками:
        B_low  — нижняя граница зоны роста throughput
        B_peak — оптимум (предсказанный максимум throughput)
        B_high — верхняя граница зоны насыщения / начало перегрузки

    Каждое наблюдение (batch_size → throughput) обновляет ближайшую
    опорную точку через EMA. Раз в exploration_interval итераций алгоритм
    делает 1–2 шага в сторону от B_peak, чтобы модель не устаревала.
    """

    type: TaskBatchProviderType = TaskBatchProviderType.ADAPTIVE_MODEL

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 # Cold start
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 2.0,
                 # Сглаживание опорных точек модели: отдельный alpha для каждой зоны.
                 # B_high обновляется агрессивнее — перегрузку нужно замечать быстро.
                 model_alpha_low: float = 0.2,
                 model_alpha_peak: float = 0.3,
                 model_alpha_high: float = 0.5,
                 # Сглаживание throughput наблюдений
                 throughput_ema_alpha: float = 0.3,
                 # Эксплорация: каждые N итераций делаем шаг в сторону
                 exploration_interval: int = 5,
                 # Размер шага эксплорации как доля от (B_high - B_low)
                 exploration_step_fraction: float = 0.15,
                 # Минимальная ширина модели (защита от схлопывания B_low == B_peak == B_high)
                 min_model_width: int = 4,
                 # Порог overload_rate: доля отказов от in_flight выше которой — перегрузка
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

        # Опорные точки модели (инициализируются при выходе из cold start)
        self._b_low: float = 0.0
        self._b_peak: float = 0.0
        self._b_high: float = 0.0

        # Throughput в опорных точках (EMA наблюдений вблизи каждой точки)
        self._t_low: float = 0.0
        self._t_peak: float = 0.0
        self._t_high: float = 0.0

        self._batch_size: float = 0.0
        self._ema_throughput: float = 0.0
        self._last_completed_count: int = 0
        self._calls_count: int = 0
        self._exploitation_calls: int = 0  # счётчик итераций с последней эксплорации

        # Направление текущей эксплорации (+1 вправо, -1 влево)
        self._exploration_direction: int = 1
        self._exploration_steps_done: int = 0

    # ── Вспомогательные методы ────────────────────────────────────────────────

    def _update_throughput_ema(self, completed_count: int) -> None:
        throughput = completed_count - self._last_completed_count
        self._ema_throughput = (self._throughput_ema_alpha * throughput
                                + (1 - self._throughput_ema_alpha) * self._ema_throughput)

    def _is_overloaded(self, overload: int, in_flight: int) -> bool:
        if in_flight == 0:
            return False
        return (overload / in_flight) > self._overload_rate_threshold

    def _assign_observation_to_zone(self, batch: float, throughput: float) -> None:
        """
        Обновляет ближайшую опорную точку модели по новому наблюдению.
        Зона определяется по положению batch относительно текущих границ.

        Логика обновления:
          - batch < midpoint(B_low, B_peak)  → зона роста, обновляем B_low / T_low
          - batch > midpoint(B_peak, B_high) → зона перегрузки, обновляем B_high / T_high
          - иначе                            → зона насыщения, обновляем B_peak / T_peak
        """
        mid_low  = (self._b_low + self._b_peak) / 2
        mid_high = (self._b_peak + self._b_high) / 2

        if batch <= mid_low:
            self._b_low  = (1 - self._model_alpha_low)  * self._b_low  + self._model_alpha_low  * batch
            self._t_low  = (1 - self._model_alpha_low)  * self._t_low  + self._model_alpha_low  * throughput
        elif batch >= mid_high:
            self._b_high = (1 - self._model_alpha_high) * self._b_high + self._model_alpha_high * batch
            self._t_high = (1 - self._model_alpha_high) * self._t_high + self._model_alpha_high * throughput
        else:
            self._b_peak = (1 - self._model_alpha_peak) * self._b_peak + self._model_alpha_peak * batch
            self._t_peak = (1 - self._model_alpha_peak) * self._t_peak + self._model_alpha_peak * throughput

        self._enforce_model_consistency()

    def _enforce_model_consistency(self) -> None:
        """
        Гарантирует B_low < B_peak < B_high с минимальным зазором.
        Если модель схлопнулась (например, после серии перегрузок загнавших
        B_high вниз) — расширяем симметрично от B_peak.
        """
        half = self._min_model_width / 2
        self._b_low  = min(self._b_low,  self._b_peak - half)
        self._b_high = max(self._b_high, self._b_peak + half)
        self._b_low  = max(1.0, self._b_low)

    def _predict_best_batch(self) -> float:
        """
        Возвращает предсказанный оптимум из текущей модели.
        Если T_peak ниже T_low — модель ещё не устоялась, возвращаем середину
        между B_peak и B_high (пробуем чуть правее).
        Если T_high выше T_peak — модель инвертирована из-за шума, держимся у B_peak.
        """
        if self._t_peak >= self._t_low and self._t_peak >= self._t_high:
            # Нормальная ситуация: B_peak — лучшая точка
            return self._b_peak
        elif self._t_low > self._t_peak:
            # Throughput растёт — оптимум правее, двигаемся к B_peak
            return (self._b_peak + self._b_high) / 2
        else:
            # T_high > T_peak — аномалия / шум, консервативно остаёмся у B_peak
            return self._b_peak

    def _exploration_step_size(self) -> float:
        return max(1.0, self._exploration_step_fraction * (self._b_high - self._b_low))

    # ── Основной метод ────────────────────────────────────────────────────────

    def calculate_batch_size(self) -> int:
        completed_count   = self._task_run_metric_provider.get_completed_count()
        temp_error_count  = self._task_run_metric_provider.get_temp_error_count_total()
        interrupted_count = self._task_run_metric_provider.get_interrupted_count_total()
        execution_count   = self._task_run_metric_provider.get_execution_count_total()
        queued_count      = self._task_run_metric_provider.get_queued_count_total()
        waiting_count     = self._task_run_metric_provider.get_waiting_count_total()

        self._update_throughput_ema(completed_count)

        overload  = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count
        in_tail   = waiting_count < max(self._batch_size, 1)
        is_overloaded = self._is_overloaded(overload, in_flight)

        self._calls_count += 1

        # ── Cold start ────────────────────────────────────────────────────────
        if self._state == AdaptiveModelState.COLD_START:
            if self._calls_count == 1:
                if not self._cold_start_batch_size:
                    self._batch_size = max(1.0, self._task_run_metric_provider.get_total_count() // 10)
                else:
                    self._batch_size = float(self._cold_start_batch_size)
            else:
                if is_overloaded or in_flight > self._ema_throughput * 3:
                    # Выходим из cold start: инициализируем модель по найденным границам.
                    # B_high — текущий (уже перегруженный) батч
                    # B_peak — середина
                    # B_low  — четверть
                    b_high = self._batch_size
                    b_low  = max(1.0, self._batch_size / 4)
                    b_peak = (b_low + b_high) / 2

                    self._b_low  = b_low
                    self._b_peak = b_peak
                    self._b_high = b_high

                    # Throughput опорных точек пока неизвестен — берём текущий EMA
                    # как нейтральный старт; модель уточнит их через наблюдения
                    self._t_low  = self._ema_throughput * 0.5
                    self._t_peak = self._ema_throughput
                    self._t_high = self._ema_throughput * 0.5

                    self._batch_size = b_peak
                    self._exploitation_calls = 0
                    self._state = AdaptiveModelState.EXPLOITATION
                else:
                    self._batch_size *= self._cold_start_growth_multiplier

        # ── Exploitation: идём в предсказанный оптимум ────────────────────────
        elif self._state == AdaptiveModelState.EXPLOITATION:
            if not in_tail:
                # Обновляем модель текущим наблюдением
                self._assign_observation_to_zone(self._batch_size, self._ema_throughput)

                # При перегрузке — принудительно сдвигаем B_high вниз и уходим к B_peak
                if is_overloaded:
                    self._b_high = max(self._b_peak + self._min_model_width,
                                       self._batch_size * 0.85)
                    self._t_high = self._ema_throughput * 0.5
                    self._enforce_model_consistency()

                self._exploitation_calls += 1

                # Пора исследовать?
                if self._exploitation_calls >= self._exploration_interval:
                    # Чередуем направление эксплорации: сначала вправо, потом влево
                    self._exploration_direction = 1 if (self._exploitation_calls // self._exploration_interval) % 2 == 1 else -1
                    self._exploration_steps_done = 0
                    self._exploitation_calls = 0
                    self._state = AdaptiveModelState.EXPLORATION
                    self._batch_size = self._predict_best_batch() + (
                        self._exploration_direction * self._exploration_step_size()
                    )
                else:
                    self._batch_size = self._predict_best_batch()

        # ── Exploration: делаем 1–2 шага в сторону, собираем наблюдение ──────
        elif self._state == AdaptiveModelState.EXPLORATION:
            if not in_tail:
                # Записываем наблюдение из точки эксплорации в модель
                self._assign_observation_to_zone(self._batch_size, self._ema_throughput)

                if is_overloaded:
                    # Наткнулись на перегрузку при эксплорации — сразу возвращаемся
                    self._b_high = max(self._b_peak + self._min_model_width,
                                       self._batch_size * 0.85)
                    self._t_high = self._ema_throughput * 0.5
                    self._enforce_model_consistency()
                    self._state = AdaptiveModelState.EXPLOITATION
                    self._batch_size = self._predict_best_batch()
                else:
                    self._exploration_steps_done += 1
                    if self._exploration_steps_done < 2:
                        # Делаем второй шаг в том же направлении
                        self._batch_size += self._exploration_direction * self._exploration_step_size()
                        self._batch_size = max(1.0, self._batch_size)
                    else:
                        # Эксплорация завершена — возвращаемся к оптимуму
                        self._state = AdaptiveModelState.EXPLOITATION
                        self._batch_size = self._predict_best_batch()

        else:
            raise RuntimeError(f"Unknown state: {self._state}")

        self._last_completed_count = completed_count
        # Никогда не выходим за границы известной безопасной зоны
        self._batch_size = max(1.0, min(self._batch_size, self._b_high))
        return int(self._batch_size)

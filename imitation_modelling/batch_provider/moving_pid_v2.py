import enum
import math

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class MovingPIDV2State(str, enum.Enum):
    COLD_START = "COLD_START"
    RANGE_RETENTION = "RANGE_RETENTION"
    ADJUSTMENT = "ADJUSTMENT"


class MovingPIDV2Provider(TaskBatchProvider):
    """
    MOVING_PID v2 — улучшенная версия с тремя ключевыми изменениями
    относительно v1:

    1. Память о перегрузках (overload_ceiling).
       Алгоритм запоминает наименьший батч при котором была перегрузка.
       Граница B_max никогда не поднимается выше overload_ceiling * safety_margin.
       Это устраняет петлю "сжались → выросли → перегрузка → сжались".

    2. Адаптивный grow_multiplier.
       После каждой перегрузки grow_multiplier штрафуется (*penalty_factor < 1).
       После каждого стабильного цикла без перегрузок — восстанавливается
       (*recovery_factor > 1) но не превышает исходное значение.
       Алгоритм автоматически становится консервативнее в нестабильных системах
       и агрессивнее в стабильных.

    3. Защита от схлопывания.
       B_min никогда не опускается ниже min_batch_floor.
       Если диапазон [B_min, B_max] схлопнулся (B_max - B_min < min_range_width),
       принудительно расширяем его от текущего B_peak.
    """

    type: TaskBatchProviderType = TaskBatchProviderType.MOVING_PID_V2

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,

                 # Cold start
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 1.5,

                 # Базовые множители диапазона
                 # grow  > 1: сдвиг вправо когда throughput растёт
                 # shrink < 1: сдвиг влево при насыщении
                 # shrink_on_overload < shrink: агрессивное сжатие при перегрузке
                 adjustment_grow_multiplier: float = 1.3,  # намеренно меньше чем в v1
                 adjustment_shrink_multiplier: float = 0.85,
                 adjustment_shrink_on_overload: float = 0.6,  # быстрое отступление при перегрузке

                 # Адаптивный grow_multiplier
                 grow_penalty_factor: float = 0.80,  # штраф после перегрузки
                 grow_recovery_factor: float = 1.05,  # восстановление после стабильного цикла
                 grow_multiplier_min: float = 1.05,  # нижний предел (алгоритм всегда растёт)
                 # верхний предел = adjustment_grow_multiplier (исходное значение)

                 # Память о перегрузках
                 # B_max не поднимается выше last_overload_batch * safety_margin
                 overload_ceiling_safety_margin: float = 0.90,
                 # Забываем потолок если долго не было перегрузок (стабильных циклов)
                 overload_ceiling_forget_after: int = 5,

                 # Защита от схлопывания диапазона
                 min_batch_floor: int = 1,
                 min_range_width: int = 4,  # минимальная ширина [B_min, B_max]

                 # RANGE_RETENTION
                 range_retention_iterations: int = 3,

                 # Порог роста EMA throughput
                 throughput_growth_threshold: float = 0.05,

                 # EMA throughput
                 throughput_ema_alpha: float = 0.3,
                 ):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)

        self._state = MovingPIDV2State.COLD_START

        # Cold start
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier

        # Множители — grow адаптивный, остальные фиксированные
        self._grow_multiplier_base = adjustment_grow_multiplier
        self._grow_multiplier = adjustment_grow_multiplier  # текущий
        self._shrink_multiplier = adjustment_shrink_multiplier
        self._shrink_on_overload = adjustment_shrink_on_overload

        # Адаптация grow_multiplier
        self._grow_penalty_factor = grow_penalty_factor
        self._grow_recovery_factor = grow_recovery_factor
        self._grow_multiplier_min = grow_multiplier_min

        # Память о перегрузках
        self._overload_ceiling = math.inf  # неизвестен до первой перегрузки
        self._overload_ceiling_margin = overload_ceiling_safety_margin
        self._overload_ceiling_forget_after = overload_ceiling_forget_after
        self._stable_cycles_since_overload = 0  # счётчик стабильных циклов

        # Защита от схлопывания
        self._min_batch_floor = min_batch_floor
        self._min_range_width = min_range_width

        # Диапазон
        self._batch_size: float = 0.0
        self._batch_size_min: float = 0.0
        self._batch_size_max: float = 0.0

        # EMA throughput
        self._throughput_ema_alpha = throughput_ema_alpha
        self._ema_throughput = 0.0
        self._prev_ema_throughput = 0.0  # EMA на момент входа в последний RANGE_RETENTION
        self._throughput_growth_threshold = throughput_growth_threshold

        self._last_completed_count = 0
        self._calls_count = 0
        self._range_retention_iterations = range_retention_iterations
        self._range_retention_calls = 0

    # ── Вспомогательные методы ────────────────────────────────────────────────

    def _update_ema(self, completed_count: int) -> None:
        throughput = completed_count - self._last_completed_count
        self._ema_throughput = (
                self._throughput_ema_alpha * throughput
                + (1 - self._throughput_ema_alpha) * self._ema_throughput
        )

    def _clamp_to_range(self, value: float) -> float:
        return max(self._batch_size_min, min(self._batch_size_max, value))

    def _apply_overload_ceiling(self, batch: float) -> float:
        """Не даём B_max подниматься выше известного потолка перегрузки."""
        if self._overload_ceiling < math.inf:
            safe_max = self._overload_ceiling * self._overload_ceiling_margin
            return min(batch, safe_max)
        return batch

    def _enforce_range_consistency(self) -> None:
        """
        Гарантирует B_min < B_max с минимальным зазором.
        Если диапазон схлопнулся — расширяем симметрично от текущего батча.
        """
        self._batch_size_min = max(self._min_batch_floor, self._batch_size_min)
        self._batch_size_max = self._apply_overload_ceiling(self._batch_size_max)
        self._batch_size_max = max(self._batch_size_min + 1, self._batch_size_max)

        if self._batch_size_max - self._batch_size_min < self._min_range_width:
            center = (self._batch_size_min + self._batch_size_max) / 2
            half = self._min_range_width / 2
            self._batch_size_min = max(self._min_batch_floor, center - half)
            self._batch_size_max = self._apply_overload_ceiling(center + half)
            # Если потолок не даёт расшириться вправо — расширяемся влево
            if self._batch_size_max - self._batch_size_min < self._min_range_width:
                self._batch_size_min = max(
                    self._min_batch_floor,
                    self._batch_size_max - self._min_range_width,
                )

    def _register_overload(self, batch: float) -> None:
        """Обновляем потолок перегрузки и штрафуем grow_multiplier."""
        # Потолок = минимум из всех наблюдавшихся батчей при перегрузке
        self._overload_ceiling = min(self._overload_ceiling, batch)
        # Штрафуем grow_multiplier, но не ниже минимума
        self._grow_multiplier = max(
            self._grow_multiplier_min,
            self._grow_multiplier * self._grow_penalty_factor,
        )
        self._stable_cycles_since_overload = 0

    def _register_stable_cycle(self) -> None:
        """Восстанавливаем grow_multiplier после стабильного цикла."""
        self._stable_cycles_since_overload += 1
        self._grow_multiplier = min(
            self._grow_multiplier_base,
            self._grow_multiplier * self._grow_recovery_factor,
        )
        # Забываем потолок если долго не было перегрузок
        if self._stable_cycles_since_overload >= self._overload_ceiling_forget_after:
            self._overload_ceiling = math.inf
            self._stable_cycles_since_overload = 0

    # ── Основной метод ────────────────────────────────────────────────────────

    def calculate_batch_size(self) -> int:
        completed_count = self._task_run_metric_provider.get_completed_count()
        temp_error_count = self._task_run_metric_provider.get_temp_error_count_total()
        interrupted_count = self._task_run_metric_provider.get_interrupted_count_total()
        execution_count = self._task_run_metric_provider.get_execution_count_total()
        queued_count = self._task_run_metric_provider.get_queued_count_total()
        waiting_count = self._task_run_metric_provider.get_waiting_count_total()

        self._update_ema(completed_count)

        overload = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count
        in_tail = waiting_count < max(self._batch_size, 1)

        self._calls_count += 1

        # ── COLD START ────────────────────────────────────────────────────────
        if self._state == MovingPIDV2State.COLD_START:
            if self._calls_count == 1:
                if not self._cold_start_batch_size:
                    self._batch_size = max(1.0, self._task_run_metric_provider.get_total_count() // 10)
                else:
                    self._batch_size = float(self._cold_start_batch_size)
            else:
                if overload > 0 or in_flight > self._ema_throughput * 3:
                    # Нашли первый признак перегрузки — инициализируем диапазон
                    self._register_overload(self._batch_size)

                    self._batch_size_max = max(self._batch_size / 2, 1.0)
                    self._batch_size_min = max(self._batch_size / 4, 1.0)
                    self._batch_size = (self._batch_size_min + self._batch_size_max) / 2

                    self._enforce_range_consistency()
                    self._prev_ema_throughput = self._ema_throughput
                    self._range_retention_calls = 0
                    self._state = MovingPIDV2State.RANGE_RETENTION
                else:
                    self._batch_size *= self._cold_start_growth_multiplier

        # ── RANGE_RETENTION ───────────────────────────────────────────────────
        elif self._state == MovingPIDV2State.RANGE_RETENTION:
            self._range_retention_calls += 1

            if overload > 0 and not in_tail:
                # Перегрузка прямо сейчас — агрессивно сжимаемся и обновляем потолок
                self._register_overload(self._batch_size)

                self._batch_size_max = max(
                    self._batch_size_min + 1,
                    self._batch_size * self._shrink_on_overload,
                )
                self._batch_size_min = max(
                    self._min_batch_floor,
                    self._batch_size_min * self._shrink_on_overload,
                )
                self._enforce_range_consistency()
                self._batch_size = self._clamp_to_range(
                    (self._batch_size_min + self._batch_size_max) / 2
                )
                # Сбрасываем счётчик — начинаем наблюдение заново
                self._range_retention_calls = 0
                self._prev_ema_throughput = self._ema_throughput

            elif not in_tail:
                self._batch_size = self._clamp_to_range(
                    (self._batch_size_min + self._batch_size_max) / 2
                )

                if self._range_retention_calls >= self._range_retention_iterations:
                    self._state = MovingPIDV2State.ADJUSTMENT

        # ── ADJUSTMENT ────────────────────────────────────────────────────────
        elif self._state == MovingPIDV2State.ADJUSTMENT:
            throughput_grew = (
                    self._ema_throughput
                    > self._prev_ema_throughput * (1 + self._throughput_growth_threshold)
            )

            if overload > 0 and not in_tail:
                # Перегрузка в момент корректировки — агрессивно влево
                self._register_overload(self._batch_size)

                self._batch_size_max = max(
                    self._batch_size_min + 1,
                    self._batch_size_max * self._shrink_on_overload,
                )
                self._batch_size_min = max(
                    self._min_batch_floor,
                    self._batch_size_min * self._shrink_on_overload,
                )

            elif throughput_grew:
                # Throughput растёт — сдвигаем вправо с адаптивным multiplier
                new_max = self._apply_overload_ceiling(
                    self._batch_size_max * self._grow_multiplier
                )
                new_min = self._apply_overload_ceiling(
                    self._batch_size_min * self._grow_multiplier
                )
                self._batch_size_max = new_max
                self._batch_size_min = new_min
                self._register_stable_cycle()

            else:
                # Насыщение — сдвигаем влево умеренно
                self._batch_size_max = max(
                    self._batch_size_min + 1,
                    self._batch_size_max * self._shrink_multiplier,
                )
                self._batch_size_min = max(
                    self._min_batch_floor,
                    self._batch_size_min * self._shrink_multiplier,
                )
                self._register_stable_cycle()

            self._enforce_range_consistency()
            self._prev_ema_throughput = self._ema_throughput
            self._range_retention_calls = 0
            self._batch_size = self._clamp_to_range(
                (self._batch_size_min + self._batch_size_max) / 2
            )
            self._state = MovingPIDV2State.RANGE_RETENTION

        else:
            raise RuntimeError(f"Unknown state: {self._state}")

        self._last_completed_count = completed_count

        # Финальная защита: никогда не выходим за потолок перегрузки
        if self._overload_ceiling < math.inf:
            self._batch_size = min(
                self._batch_size,
                self._overload_ceiling * self._overload_ceiling_margin,
            )

        return max(1, int(self._batch_size))

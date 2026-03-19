import enum

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class MovingPIDState(str, enum.Enum):
    COLD_START = "COLD_START"
    RANGE_RETENTION = "RANGE_RETENTION"
    ADJUSTMENT = "ADJUSTMENT"


class MovingPIDProvider(TaskBatchProvider):
    type: TaskBatchProviderType.MOVING_PID

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 1.5,
                 # Сколько итераций удерживаем диапазон перед корректировкой
                 range_retention_iterations: int = 3,
                 # Множитель сдвига вправо (throughput растёт)
                 adjustment_grow_multiplier: float = 1.5,
                 # Множитель сдвига влево (насыщение / деградация)
                 adjustment_shrink_multiplier: float = 0.8,
                 # Порог "роста" EMA throughput — считаем ростом, если новый EMA больше старого на X%
                 throughput_growth_threshold: float = 0.05):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._state = MovingPIDState.COLD_START
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier
        self._range_retention_iterations = range_retention_iterations
        self._adjustment_grow_multiplier = adjustment_grow_multiplier
        self._adjustment_shrink_multiplier = adjustment_shrink_multiplier
        self._throughput_growth_threshold = throughput_growth_threshold

        self._batch_size = None
        self._batch_size_min = None
        self._batch_size_max = None

        self._ema_throughput = 0
        self._prev_ema_throughput = 0  # EMA на момент входа в RANGE_RETENTION, для сравнения в ADJUSTMENT
        self._last_completed_count = 0
        self._last_call_at = 0
        self._calls_count = 0

        # Счётчик итераций внутри RANGE_RETENTION
        self._range_retention_calls = 0

    def _clamp_to_range(self, value: int) -> int:
        """Удерживаем значение в диапазоне [B_min, B_max]."""
        return max(self._batch_size_min, min(self._batch_size_max, value))

    def calculate_batch_size(self) -> int:
        execution_count = self._task_run_metric_provider.get_execution_count_total()
        queued_count = self._task_run_metric_provider.get_queued_count_total()
        temp_error_count = self._task_run_metric_provider.get_temp_error_count_total()
        interrupted_count = self._task_run_metric_provider.get_interrupted_count_total()
        completed_count = self._task_run_metric_provider.get_completed_count()

        throughput = completed_count - self._last_completed_count
        self._ema_throughput = 0.3 * throughput + 0.7 * self._ema_throughput
        overload = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count

        self._last_completed_count = completed_count
        self._calls_count += 1

        # ------------------------------------------------------------------ #
        # COLD START                                                         #
        # ------------------------------------------------------------------ #
        if self._state == MovingPIDState.COLD_START:
            if self._calls_count == 1:
                if not self._cold_start_batch_size:
                    self._batch_size = max(1, self._task_run_metric_provider.get_total_count() // 10)
                else:
                    self._batch_size = self._cold_start_batch_size
            else:
                if overload > 0 or in_flight > self._ema_throughput * 3:
                    # Нашли первый признак перегрузки — фиксируем диапазон и переходим к удержанию
                    self._batch_size_min = max(self._batch_size // 2, 1)
                    self._batch_size_max = self._batch_size
                    self._batch_size = max((self._batch_size_min + self._batch_size_max) // 2, 1)

                    # Запоминаем EMA в момент перехода, чтобы было с чем сравнивать в ADJUSTMENT
                    self._prev_ema_throughput = self._ema_throughput
                    self._range_retention_calls = 0
                    self._state = MovingPIDState.RANGE_RETENTION
                else:
                    self._batch_size = int(self._batch_size * self._cold_start_growth_multiplier)

        # ------------------------------------------------------------------ #
        # RANGE RETENTION                                                    #
        # Цель: поработать в найденном диапазоне достаточное число итераций, #
        # накопить стабильную EMA, затем решить — расширяться или сужаться.  #
        # ------------------------------------------------------------------ #
        elif self._state == MovingPIDState.RANGE_RETENTION:
            self._range_retention_calls += 1

            if overload > 0:
                # Прямо сейчас перегрузка — немедленно сдвигаем диапазон влево,
                # не ждём окончания периода удержания
                self._batch_size_max = max(self._batch_size_min + 1,
                                           int(self._batch_size_max * self._adjustment_shrink_multiplier))
                self._batch_size_min = max(1, int(self._batch_size_min * self._adjustment_shrink_multiplier))
                self._batch_size = self._clamp_to_range(
                    max((self._batch_size_min + self._batch_size_max) // 2, 1)
                )
                # Начинаем отсчёт удержания заново
                self._range_retention_calls = 0
                self._prev_ema_throughput = self._ema_throughput
            else:
                # Удерживаем батч в диапазоне: берём середину
                self._batch_size = self._clamp_to_range(
                    (self._batch_size_min + self._batch_size_max) // 2
                )

                if self._range_retention_calls >= self._range_retention_iterations:
                    # Накопили достаточно данных — переходим к корректировке диапазона
                    self._state = MovingPIDState.ADJUSTMENT

        # ------------------------------------------------------------------ #
        # ADJUSTMENT                                                         #
        # Цель: сдвинуть диапазон [B_min, B_max] в сторону лучшей            #
        # пропускной способности и вернуться в RANGE_RETENTION.              #
        # ------------------------------------------------------------------ #
        elif self._state == MovingPIDState.ADJUSTMENT:
            throughput_grew = (
                self._ema_throughput > self._prev_ema_throughput * (1 + self._throughput_growth_threshold)
            )

            if overload > 0:
                # Перегрузка — сдвигаем влево (сужаемся)
                self._batch_size_max = max(self._batch_size_min + 1,
                                           int(self._batch_size_max * self._adjustment_shrink_multiplier))
                self._batch_size_min = max(1, int(self._batch_size_min * self._adjustment_shrink_multiplier))
            elif throughput_grew:
                # Пропускная способность растёт — есть потенциал, сдвигаем вправо
                self._batch_size_min = int(self._batch_size_min * self._adjustment_grow_multiplier)
                self._batch_size_max = int(self._batch_size_max * self._adjustment_grow_multiplier)
            else:
                # Насыщение или деградация — сдвигаем влево
                self._batch_size_max = max(self._batch_size_min + 1,
                                           int(self._batch_size_max * self._adjustment_shrink_multiplier))
                self._batch_size_min = max(1, int(self._batch_size_min * self._adjustment_shrink_multiplier))

            # Запоминаем EMA как базу для следующего цикла сравнения
            self._prev_ema_throughput = self._ema_throughput

            # Возвращаемся в RANGE_RETENTION — накапливаем статистику в новом диапазоне
            self._range_retention_calls = 0
            self._batch_size = self._clamp_to_range(
                (self._batch_size_min + self._batch_size_max) // 2
            )
            self._state = MovingPIDState.RANGE_RETENTION

        else:
            raise RuntimeError(f"Unknown state: {self._state}")

        return int(self._batch_size)

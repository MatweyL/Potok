from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import SystemTime, TaskBatchProviderType
from imitation_modelling.task_batch_provider import TaskBatchProvider
from service.ports.common.logs import logger


class AIMDTaskBatchProvider(TaskBatchProvider):
    type: TaskBatchProviderType = TaskBatchProviderType.AIMD

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 delta: int,  # Аддитивный шаг (например, 5)
                 beta: float,  # Мультипликативный коэффициент (например, 0.7)
                 base_batch_size: int,
                 batch_size_min: int,
                 batch_size_max: int,
                 throughput_alpha: float = 0.3):  # Сглаживание для пропускной способности
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self.delta = delta
        self.beta = beta
        self.batch_size_min = batch_size_min
        self.batch_size_max = batch_size_max

        self.current_batch_size = float(base_batch_size)
        self.ema_throughput = 0.0
        self.throughput_alpha = throughput_alpha
        self.last_completed = 0

    def calculate_batch_size(self) -> int:
        metrics = self._task_run_metric_provider
        completed = metrics.get_completed_count()

        # 1. Считаем реальную пропускную способность (сколько задач реально "вышло")
        instant_throughput = completed - self.last_completed
        self.ema_throughput = (self.throughput_alpha * instant_throughput +
                               (1 - self.throughput_alpha) * self.ema_throughput)
        self.last_completed = completed

        # 2. Собираем сигналы о перегрузке
        errors = metrics.get_temp_error_count_total() + metrics.get_interrupted_count_total()
        in_flight = metrics.get_execution_count_total() + metrics.get_queued_count_total()

        # 3. Основная логика AIMD

        # СИГНАЛ ТОРМОЖЕНИЯ (Multiplicative Decrease)
        # Если есть ошибки ИЛИ мы отправили в 3 раза больше задач, чем реально успеваем обработать
        if errors > 0 or in_flight > self.ema_throughput * 3:
            self.current_batch_size *= self.beta
            # Сбрасываем EMA, чтобы не "памятовать" старые успехи в момент аварии
            self.ema_throughput *= self.beta

            # СИГНАЛ РОСТА (Additive Increase)
        else:
            # Растем только если:
            # а) Текущий батч не сильно превышает реальный throughput (есть куда расти)
            # б) Мы не наткнулись на плато (опционально можно добавить сравнение с prev_ema)
            if self.current_batch_size < self.ema_throughput * 1.5:
                self.current_batch_size += self.delta
            else:
                # Насыщение: батч уже большой, а throughput не догоняет.
                # Не увеличиваем, просто "ждем" систему.
                pass

        # 4. Ограничения
        self.current_batch_size = max(self.batch_size_min,
                                      min(self.current_batch_size, self.batch_size_max))

        return int(self.current_batch_size)
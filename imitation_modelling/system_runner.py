from queue import Queue
from time import perf_counter

from imitation_modelling.broker import Broker
from imitation_modelling.handler import HandlerPool, RandomTimeoutGenerator, Handler
from imitation_modelling.metric_collector import MetricCollector
from imitation_modelling.repo import TaskRunMetricProvider, TaskRunStatusRepo
from imitation_modelling.schemas import SystemTime, TaskRunStatusLog, TaskRunStatus, SimulationParams
from imitation_modelling.task_batch_provider_builder import TaskBatchProviderBuilder
from imitation_modelling.task_manager import TaskManager


class SystemRunner:
    def __init__(self, handler_pool: HandlerPool,
                 broker: Broker,
                 task_manager: TaskManager,
                 system_time: SystemTime,
                 metric_collector: MetricCollector,
                 metric_provider: TaskRunMetricProvider,
                 max_run_seconds: int = 60):
        self.handler_pool = handler_pool
        self.broker = broker
        self.task_manager = task_manager
        self.system_time = system_time
        self.metric_collector = metric_collector
        self.metric_provider = metric_provider
        self.max_run_seconds = max_run_seconds

    def run(self):
        was_25_percent_progress = False
        was_50_percent_progress = False
        was_75_percent_progress = False
        was_90_percent_progress = False
        print("started")
        start_counter = perf_counter()
        while self.metric_provider.get_completed_count() < self.metric_provider.get_total_count():
            current_value = self.metric_provider.get_completed_count() / self.metric_provider.get_total_count()
            if 0.25 <= current_value < 0.5 and not was_25_percent_progress:
                was_25_percent_progress = True
                print("progress: 25%")
            elif 0.5 <= current_value < 0.75 and not was_50_percent_progress:
                was_50_percent_progress = True
                print("progress: 50%")
            elif 0.75 <= current_value < 0.9 and not was_75_percent_progress:
                was_75_percent_progress = True
                print("progress: 75%")
            elif current_value > 0.9 and not was_90_percent_progress:
                was_90_percent_progress = True
                print("progress: 90%")
            if perf_counter() - start_counter >= self.max_run_seconds:
                print(f'{self.max_run_seconds} timeout exceed; break simulation')
                break
            self.system_time.tick()
            self.task_manager.consume_statuses()
            self.task_manager.transit_from_queued_or_execution_to_interrupted()
            self.task_manager.transit_from_temp_error_or_interrupted_to_waiting()
            self.task_manager.run_send_tasks()
            self.handler_pool.consume()
            self.handler_pool.handle_tasks_runs()
            self.metric_collector.collect()
        self.metric_collector.stop()
        print("progress: 100%")
        self.metric_collector.save()


def build_system_runner(params: SimulationParams):
    time_step_seconds = params.time_step_seconds
    system_time = SystemTime(time_step_seconds=time_step_seconds)
    broker = Broker(Queue(), Queue())
    handlers_amount = params.handlers_amount
    handler_max_tasks = params.handler_max_tasks
    execution_confirm_timeout = params.execution_confirm_timeout
    tasks_part_from_all_for_high_load = params.tasks_part_from_all_for_high_load
    temp_error_probability_at_high_load = params.temp_error_probability_at_high_load
    task_execution_timeout_generator = RandomTimeoutGenerator(params.random_timeout_generator_left,
                                                              params.random_timeout_generator_right)
    handler_pool = HandlerPool(broker, [Handler(str(i), broker, system_time, task_execution_timeout_generator,
                                                handler_max_tasks, execution_confirm_timeout,
                                                tasks_part_from_all_for_high_load,
                                                temp_error_probability_at_high_load) for i in range(handlers_amount)])

    tasks_amount = params.tasks_amount
    task_run_status_logs_by_task_run_id = {str(i): [TaskRunStatusLog(task_run_id=str(i),
                                                                     status=TaskRunStatus.WAITING,
                                                                     created_timestamp=system_time.current, )] for i in
                                           range(tasks_amount)}
    interrupted_timeout = execution_confirm_timeout + 10
    run_timeout = params.run_timeout
    metric_provider_period = params.metric_provider_period
    task_run_status_repo = TaskRunStatusRepo(system_time, task_run_status_logs_by_task_run_id)
    metric_provider = TaskRunMetricProvider(task_run_status_repo, metric_provider_period)
    metric_collector = MetricCollector(system_time, metric_provider, params)
    task_batch_provider_builder = TaskBatchProviderBuilder(broker,
                                                           task_run_status_repo,
                                                           metric_provider,
                                                           system_time,
                                                           metric_collector)
    task_batch_provider = task_batch_provider_builder.build(params.task_batch_provider_type,
                                                            params.task_batch_provider_params)
    task_manager = TaskManager(interrupted_timeout, broker, system_time, run_timeout,
                               task_batch_provider,
                               task_run_status_repo, )
    system_runner = SystemRunner(handler_pool, broker, task_manager, system_time, metric_collector, metric_provider,
                                 params.max_run_seconds,)
    return system_runner

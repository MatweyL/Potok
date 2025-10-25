from datetime import timedelta

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo
from imitation_modelling.schemas import SystemTime, TaskRunStatusLog, TaskRunStatus, TaskRun
from imitation_modelling.task_batch_provider import TaskBatchProvider


class TaskManager:

    def __init__(self,
                 interrupted_timeout: int,
                 broker: Broker,
                 system_time: SystemTime,
                 run_timeout: int,
                 task_batch_provider: TaskBatchProvider,
                 task_run_status_repo: TaskRunStatusRepo,):
        self._interrupted_timeout = timedelta(seconds=interrupted_timeout)
        self._run_timeout = timedelta(seconds=run_timeout)
        self._task_batch_provider = task_batch_provider
        self._task_run_status_repo = task_run_status_repo
        self._broker = broker
        self.system_time = system_time
        self.next_send_tasks_time = self.system_time.current

    def consume_statuses(self):
        while True:
            task_run_status_log = self._broker.get_task_run_status_log()
            if not task_run_status_log:
                break
            self._task_run_status_repo.add(task_run_status_log)

    def run_send_tasks(self):
        if self.next_send_tasks_time <= self.system_time.current:
            self.send_tasks()
            self.next_send_tasks_time = self.system_time.current + self._run_timeout

    def send_tasks(self):
        for task_run_status_log in self._task_batch_provider.iter():
            self._task_run_status_repo.add(TaskRunStatusLog(task_run_id=task_run_status_log.task_run_id,
                                                            status=TaskRunStatus.QUEUED,
                                                            created_timestamp=self.system_time.current))
            self._broker.send_task_run(TaskRun(id=task_run_status_log.task_run_id))

    def transit_from_temp_error_or_interrupted_to_waiting(self):
        for actual_task_run_status_log in self._task_run_status_repo.iter_actual_statuses({TaskRunStatus.TEMP_ERROR,
                                                                                           TaskRunStatus.INTERRUPTED}):
            self._task_run_status_repo.add(TaskRunStatusLog(task_run_id=actual_task_run_status_log.task_run_id,
                                                            status=TaskRunStatus.WAITING,
                                                            created_timestamp=self.system_time.current))

    def transit_from_queued_or_execution_to_interrupted(self):
        for actual_task_run_status_log in self._task_run_status_repo.iter_actual_statuses({TaskRunStatus.QUEUED,
                                                                                           TaskRunStatus.EXECUTION}):
            not_updated_for_a_long_time = self.system_time.current - actual_task_run_status_log.created_timestamp > self._interrupted_timeout
            if not_updated_for_a_long_time:
                self._task_run_status_repo.add(TaskRunStatusLog(task_run_id=actual_task_run_status_log.task_run_id,
                                                                status=TaskRunStatus.INTERRUPTED,
                                                                created_timestamp=self.system_time.current))

from queue import Queue, Empty
from queue import Queue, Empty
from typing import Optional, Any

from imitation_modelling.schemas import TaskRunStatusLog, TaskRun


class Broker:
    def __init__(self, task_run_queue: Queue, task_run_status_log_queue: Queue):
        self.task_run_queue = task_run_queue
        self.task_run_status_log_queue = task_run_status_log_queue

    def send_task_run(self, task_run: TaskRun):
        self.task_run_queue.put(task_run)

    def send_task_run_status_log(self, task_run_status_log: TaskRunStatusLog):
        self.task_run_status_log_queue.put(task_run_status_log)

    def _safe_get(self, queue: Queue) -> Optional[Any]:
        try:
            obj = queue.get(block=False)
        except Empty:
            return None
        else:
            return obj

    def get_task_run(self) -> Optional[TaskRun]:
        return self._safe_get(self.task_run_queue)

    def get_task_run_status_log(self) -> Optional[TaskRunStatusLog]:
        return self._safe_get(self.task_run_status_log_queue)


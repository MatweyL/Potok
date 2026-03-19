from queue import Queue, Empty
from time import perf_counter
from typing import Optional, Any, Dict, List

from imitation_modelling.schemas import TaskRunStatusLog, TaskRun


class Broker:
    def __init__(self, task_run_queue: Queue, task_run_status_log_queue: Queue, task_ttl: int):
        self.task_run_queue = task_run_queue
        self.task_run_status_log_queue = task_run_status_log_queue
        self.task_ttl = task_ttl

        self._queued_at_list_by_task_run_id: Dict[str, List[float]] = {}

    def send_task_run(self, task_run: TaskRun):
        if task_run.id not in self._queued_at_list_by_task_run_id:
            self._queued_at_list_by_task_run_id[task_run.id] = [perf_counter()]
        else:
            self._queued_at_list_by_task_run_id[task_run.id].append(perf_counter())
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
        task_run = self._safe_get(self.task_run_queue)
        if task_run:
            queued_at_list = self._queued_at_list_by_task_run_id[task_run.id]
            if len(queued_at_list) == 1:
                self._queued_at_list_by_task_run_id.pop(task_run.id)
            queued_at = queued_at_list.pop(0)
            queued_duration = perf_counter() - queued_at
            if queued_duration >= self.task_ttl:
                return self.get_task_run()
        return task_run

    def get_task_run_status_log(self) -> Optional[TaskRunStatusLog]:
        return self._safe_get(self.task_run_status_log_queue)

import random
from abc import abstractmethod, ABC
from datetime import timedelta
from random import choice
from typing import List, Dict, Optional

from imitation_modelling.broker import Broker
from imitation_modelling.schemas import SystemTime, TaskRunStatusLog, TaskRunStatus, TaskRun, TaskExecution


class TimeoutGeneratorI(ABC):
    @abstractmethod
    def generate(self) -> float:
        pass


class RandomTimeoutGenerator(TimeoutGeneratorI):
    def __init__(self, min_seconds: float, max_seconds: float, ):
        self.min_seconds = min_seconds
        self.max_seconds = max_seconds

    def generate(self) -> float:
        return random.uniform(self.min_seconds, self.max_seconds)


class Handler:

    def __init__(self,
                 handler_id: str,
                 broker: Broker,
                 system_time: SystemTime,
                 task_execution_duration_generator: TimeoutGeneratorI,
                 max_tasks: Optional[int],
                 execution_confirm_timeout: int,
                 tasks_part_from_all_for_high_load: float,
                 temp_error_probability_at_high_load: float,
                 ):
        self.handler_id = handler_id
        self.broker = broker
        self.handling_task_by_id: Dict[str, TaskExecution] = {}
        self.max_tasks = max_tasks
        self.task_execution_duration_generator = task_execution_duration_generator
        self.execution_confirm_timeout = timedelta(seconds=execution_confirm_timeout)
        self.tasks_part_from_all_for_high_load = tasks_part_from_all_for_high_load
        self.temp_error_probability_at_high_load = temp_error_probability_at_high_load
        self.system_time = system_time

    @property
    def can_consume(self) -> bool:
        if not self.max_tasks or self.max_tasks <= 0:
            return True
        return len(self.handling_task_by_id) < self.max_tasks

    def consume(self, task_run: TaskRun):
        if not self.can_consume:
            raise Exception("Cannot consume: worker busy")
        task_run_status_log = TaskRunStatusLog(task_run_id=task_run.id,
                                               status=TaskRunStatus.EXECUTION,
                                               created_timestamp=self.system_time.current)
        self.broker.send_task_run_status_log(task_run_status_log)

        execution_duration = self.task_execution_duration_generator.generate()
        finish_time = self.system_time.current + timedelta(seconds=execution_duration)
        next_execution_confirm_time = self.system_time.current + self.execution_confirm_timeout
        task_execution = TaskExecution(task_run=task_run,
                                       finish_time=finish_time,
                                       next_execution_confirm_time=next_execution_confirm_time)

        self.handling_task_by_id[task_run.id] = task_execution

    def handle_task_runs(self):
        succeed_task_run_ids = []
        temp_error_task_run_ids = []
        execution_confirm_task_run_ids = []
        for task_run_id, task_execution in self.handling_task_by_id.items():
            if task_execution.finish_time <= self.system_time.current:
                succeed_task_run_ids.append(task_run_id)
                continue

            is_high_load = len(self.handling_task_by_id) >= int(self.max_tasks * self.tasks_part_from_all_for_high_load)
            if is_high_load and random.random() < self.temp_error_probability_at_high_load:
                temp_error_task_run_ids.append(task_run_id)
                continue
            if task_execution.next_execution_confirm_time <= self.system_time.current:
                execution_confirm_task_run_ids.append(task_run_id)

        for task_run_id in execution_confirm_task_run_ids:
            task_execution = self.handling_task_by_id[task_run_id]
            task_run_status_log = TaskRunStatusLog(task_run_id=task_execution.task_run.id,
                                                   status=TaskRunStatus.EXECUTION,
                                                   created_timestamp=self.system_time.current)
            self.broker.send_task_run_status_log(task_run_status_log)
            next_execution_confirm_time = self.system_time.current + self.execution_confirm_timeout
            task_execution.next_execution_confirm_time = next_execution_confirm_time

        task_run_ids_by_status = {TaskRunStatus.SUCCEED: succeed_task_run_ids,
                                  TaskRunStatus.TEMP_ERROR: temp_error_task_run_ids}
        for task_run_status, task_run_ids in task_run_ids_by_status.items():
            for task_run_id in task_run_ids:
                task_execution = self.handling_task_by_id[task_run_id]
                task_run_status_log = TaskRunStatusLog(task_run_id=task_execution.task_run.id,
                                                       status=task_run_status,
                                                       created_timestamp=self.system_time.current)
                self.broker.send_task_run_status_log(task_run_status_log)
                self.handling_task_by_id.pop(task_run_id)


class HandlerPool:

    def __init__(self, broker: Broker, handlers: List[Handler]):
        self.broker = broker
        self.handlers = handlers

    def get_available_handlers(self) -> List[Handler]:
        available_handlers = [handler for handler in self.handlers if handler.can_consume]
        return available_handlers

    def consume(self):
        while True:
            task_run = self.broker.get_task_run()
            if not task_run:
                break
            available_handlers = self.get_available_handlers()
            if not available_handlers:
                break
            handler = choice(available_handlers)
            handler.consume(task_run)

    def handle_tasks_runs(self):
        for handler in self.handlers:
            handler.handle_task_runs()

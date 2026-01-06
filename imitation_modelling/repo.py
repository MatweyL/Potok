from datetime import timedelta, datetime
from functools import cached_property
from typing import List, Dict, Set, Iterator

from imitation_modelling.schemas import SystemTime, TaskRunStatusLog, TaskRunStatus


class TaskRunStatusRepo:
    def __init__(self, system_time: SystemTime, task_status_log_by_task_run_id: Dict[str, List[TaskRunStatusLog]]):
        self.system_time = system_time
        self._task_status_log_by_task_run_id = task_status_log_by_task_run_id

    def delete_older_than_with_actual_keeping(self, dt: datetime, ):
        items_to_delete_by_task_run_id = {}
        for task_run_id, task_status_logs in self._task_status_log_by_task_run_id.items():
            items_to_delete = 0
            for task_run_status_log in task_status_logs:
                if task_run_status_log.created_timestamp <= dt:
                    items_to_delete += 1
                else:
                    break
            if len(task_status_logs) == items_to_delete:
                items_to_delete -= 1  # Оставляем последний и самый актуальный статус
            if items_to_delete:
                items_to_delete_by_task_run_id[task_run_id] = items_to_delete
        for task_run_id, items_to_delete in items_to_delete_by_task_run_id.items():
            task_status_logs = self._task_status_log_by_task_run_id[task_run_id]
            self._task_status_log_by_task_run_id[task_run_id] = task_status_logs[items_to_delete:]

    def add(self, task_run_status_log: TaskRunStatusLog):
        self._task_status_log_by_task_run_id[task_run_status_log.task_run_id].append(task_run_status_log)

    def iter_actual_statuses(self, task_run_statuses: Set[TaskRunStatus] = None) -> Iterator[TaskRunStatusLog]:
        if not task_run_statuses:
            for task_status_logs in self._task_status_log_by_task_run_id.values():
                yield task_status_logs[-1]
            return
        for task_status_logs in self._task_status_log_by_task_run_id.values():
            actual_task_status_log = task_status_logs[-1]
            if actual_task_status_log.status in task_run_statuses:
                yield actual_task_status_log

    def get_current_count(self, task_run_statuses: Set[TaskRunStatus] = None) -> int:
        if not task_run_statuses:
            return len(self._task_status_log_by_task_run_id)
        current_count = 0
        for task_status_logs in self._task_status_log_by_task_run_id.values():
            task_status_log = task_status_logs[-1]
            if task_status_log.status in task_run_statuses:
                current_count += 1
        return current_count

    def get_current_count_by_period(self, task_run_statuses: Set[TaskRunStatus], period: int) -> int:
        current_count = 0
        for task_status_logs in self._task_status_log_by_task_run_id.values():
            task_status_log: TaskRunStatusLog = task_status_logs[-1]
            status_in_period = task_status_log.created_timestamp + timedelta(seconds=period) > self.system_time.current
            if task_status_log.status in task_run_statuses and status_in_period:
                current_count += 1
        return current_count

    def get_total_count_by_period(self, task_run_statuses: Set[TaskRunStatus], period: int) -> int:
        total = 0
        for task_status_logs in self._task_status_log_by_task_run_id.values():
            for task_status_log in reversed(task_status_logs):
                record_in_period = task_status_log.created_timestamp + timedelta(
                    seconds=period) > self.system_time.current
                if not record_in_period:
                    break
                if task_status_log.status in task_run_statuses:
                    total += 1
        return total

    def get_average_by_period(self, task_run_status: TaskRunStatus, period: int) -> float:
        total_count = 0
        total_duration = 0
        for task_status_logs in self._task_status_log_by_task_run_id.values():
            has_suitable_status_in_period = False
            before_newest_appearance = None
            oldest_appearance = None
            status_streak_ended = False
            for index in range(len(task_status_logs) - 1, -1, -1):
                task_status_log: TaskRunStatusLog = task_status_logs[index]
                status_in_period = task_status_log.created_timestamp + timedelta(
                    seconds=period) > self.system_time.current
                if not status_in_period and not has_suitable_status_in_period:
                    break

                if task_status_log.status == task_run_status:
                    has_suitable_status_in_period = True
                    if not before_newest_appearance:
                        if index == len(task_status_logs) - 1:
                            before_newest_appearance = task_status_log
                        else:
                            before_newest_appearance = task_status_logs[index + 1]
                    oldest_appearance = task_status_log
                elif oldest_appearance:
                    status_streak_ended = True
                if has_suitable_status_in_period and oldest_appearance != before_newest_appearance and status_streak_ended:
                    if not before_newest_appearance:
                        end_time = self.system_time.current
                    else:
                        end_time = before_newest_appearance.created_timestamp
                    duration = (end_time - oldest_appearance.created_timestamp).total_seconds()
                    total_count += 1
                    total_duration += duration

                    before_newest_appearance = None
                    oldest_appearance = None
                    has_suitable_status_in_period = False
                    status_streak_ended = False

        return total_duration / total_count if total_count else 0


class TaskRunMetricProvider:

    def get_completed_count(self) -> int:
        return self._task_run_status_repo.get_current_count({TaskRunStatus.SUCCEED,
                                                             TaskRunStatus.ERROR,
                                                             TaskRunStatus.CANCELLED, })

    def get_total_count(self) -> int:
        return self._task_run_status_repo.get_current_count()

    def __init__(self, task_run_status_repo: TaskRunStatusRepo, period: int):
        self._task_run_status_repo = task_run_status_repo
        self._period = period

    @cached_property
    def metric_period(self) -> timedelta:
        return timedelta(seconds=self._period)

    def get_execution_count_total(self) -> int:
        return self._task_run_status_repo.get_current_count({TaskRunStatus.EXECUTION})

    def get_queued_count_total(self) -> int:
        return self._task_run_status_repo.get_current_count({TaskRunStatus.QUEUED})

    def get_waiting_count_total(self) -> int:
        return self._task_run_status_repo.get_current_count({TaskRunStatus.WAITING,
                                                             TaskRunStatus.INTERRUPTED,
                                                             TaskRunStatus.TEMP_ERROR})

    def get_queued_average_duration(self) -> float:
        return self._task_run_status_repo.get_average_by_period(TaskRunStatus.QUEUED,
                                                                self._period)

    def get_execution_average_duration(self) -> float:
        return self._task_run_status_repo.get_average_by_period(TaskRunStatus.EXECUTION,
                                                                self._period)

    def get_return_frequency(self) -> float:
        return self._task_run_status_repo.get_total_count_by_period({TaskRunStatus.INTERRUPTED,
                                                                     TaskRunStatus.TEMP_ERROR},
                                                                    self._period) / self._period

    def get_succeed_frequency(self) -> float:
        return self._task_run_status_repo.get_total_count_by_period({TaskRunStatus.SUCCEED,
                                                                     TaskRunStatus.ERROR,
                                                                     TaskRunStatus.CANCELLED, },
                                                                    self._period) / self._period
    def get_succeed_by_period(self) -> int:
        return self._task_run_status_repo.get_total_count_by_period({TaskRunStatus.SUCCEED,
                                                                     TaskRunStatus.ERROR,
                                                                     TaskRunStatus.CANCELLED, },
                                                                    self._period)

    def get_error_by_period(self) -> int:
        return self._task_run_status_repo.get_total_count_by_period({TaskRunStatus.INTERRUPTED,
                                                                     TaskRunStatus.TEMP_ERROR},
                                                                    self._period)
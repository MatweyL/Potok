import random
from datetime import timedelta
from typing import Dict, Tuple, List

import pytest

from imitation_modelling.repo import TaskRunStatusRepo
from imitation_modelling.schemas import TaskRunStatus, SystemTime, TaskRunStatusLog

system_time = SystemTime("2025-10-15 20:00:00")
current_time = system_time.current


class TaskRunStatusLogCreator:
    def __init__(self, task_run_id: str = None):
        self.task_run_id = task_run_id if task_run_id else str(random.random())

    def create(self, status: TaskRunStatus, after_current: int):
        created_timestamp = current_time - timedelta(seconds=after_current)
        return TaskRunStatusLog(task_run_id=self.task_run_id, status=status, created_timestamp=created_timestamp)


def create_task_run_status_logs_by_id_1() -> Tuple[Dict[str, TaskRunStatusLog], float, TaskRunStatus, int]:
    creator = TaskRunStatusLogCreator()
    return ({
                creator.task_run_id: [
                                         creator.create(TaskRunStatus.SUCCEED, 0),
                                         creator.create(TaskRunStatus.QUEUED, 10),
                                         creator.create(TaskRunStatus.WAITING, 20),
                                         creator.create(TaskRunStatus.SUCCEED, 30),
                                         creator.create(TaskRunStatus.QUEUED, 40),
                                     ][::-1]
            }, 10, TaskRunStatus.QUEUED, 60)


# [status, duration, status_appearance_number]

def create_test_case(params: List[Tuple[int, int]], status: TaskRunStatus, period: int) -> Tuple[
    Dict[str, List[TaskRunStatusLog]], float, TaskRunStatus, int]:
    creator = TaskRunStatusLogCreator()
    offset_from_current_time = 0
    logs = []
    for param in params:
        duration, status_appearance_number = param
        first_log = creator.create(TaskRunStatus.SUCCEED, offset_from_current_time)
        logs.append(first_log)
        one_status_duration = duration / status_appearance_number
        for _ in range(status_appearance_number):
            offset_from_current_time += one_status_duration
            log = creator.create(status, offset_from_current_time)
            logs.append(log)
        last_log = creator.create(TaskRunStatus.SUCCEED, offset_from_current_time)
        logs.append(last_log)

    total_duration = 0
    total_status_series_appearance = 0
    offset_from_current_time = 0
    for param in params:
        duration, status_appearance_number = param
        one_status_duration = duration / status_appearance_number
        if offset_from_current_time + one_status_duration < period:
            total_duration += duration
            total_status_series_appearance += 1
            offset_from_current_time += duration
        else:
            break
    avg_duration = total_duration / total_status_series_appearance if total_status_series_appearance else 0
    return ({creator.task_run_id: logs[::-1]}, avg_duration, status, period)


@pytest.mark.parametrize("task_run_status_logs_by_task_run_id, value, status, period", [
    create_test_case([(10, 1)], TaskRunStatus.QUEUED, 60),
    create_test_case([(20, 1), (10, 1)], TaskRunStatus.QUEUED, 60),
    create_test_case([(25, 2)], TaskRunStatus.QUEUED, 60),
    create_test_case([(40, 1), (60, 2), (1000, 1)], TaskRunStatus.QUEUED, 60),
    create_test_case([(100, 1),], TaskRunStatus.QUEUED, 60),
])
def test_get_average_by_period(task_run_status_logs_by_task_run_id, value, status, period):
    task_run_status_repo = TaskRunStatusRepo(system_time, task_run_status_logs_by_task_run_id)
    average = task_run_status_repo.get_average_by_period(status, period=period)
    assert average == value

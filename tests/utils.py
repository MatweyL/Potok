from datetime import datetime
from typing import List

from service.domain.schemas.enums import TaskStatus, TaskRunStatus
from service.domain.schemas.monitoring_algorithm import PeriodicMonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.domain.schemas.task_group import TaskGroup
from service.domain.schemas.task_run import TaskRun
from service.ports.outbound.repo.abstract import Repo


async def create_tasks(task_repo: Repo,
                       task_group_repo: Repo,
                       algorithm_repo: Repo,
                       payload_repo: Repo,
                       group_name: str,
                       tasks_amount: int,
                       task_status: TaskStatus = TaskStatus.NEW) -> List[Task]:
    algorithm = await algorithm_repo.create(PeriodicMonitoringAlgorithm(timeout=3600.0, timeout_noize=60.0))
    task_group = await task_group_repo.create( TaskGroup(name=group_name, title='', description=''))
    payload = await payload_repo.create(Payload(data={}))
    tasks = [Task(group_id=task_group.id,monitoring_algorithm_id=algorithm.id,status=task_status,
                  status_updated_at=datetime.now(), payload_id=payload.id) for i in range(tasks_amount)]
    tasks = await task_repo.create_all(tasks)
    return tasks


async def create_tasks_runs(task_run_repo: Repo,
                            tasks: List[Task],
                            group_name: str,
                            task_run_status: TaskRunStatus,) -> List[TaskRun]:
    return await task_run_repo.create_all([TaskRun(task_id=task.id,
                                            group_name=group_name,
                                            status=task_run_status,
                                            status_updated_at=task.status_updated_at) for task in tasks])
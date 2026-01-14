from datetime import datetime, timedelta
from typing import Callable, Awaitable

import pytest
import pytest_asyncio

from service.domain.schemas.enums import PriorityType, TaskType, TaskStatus, MonitoringAlgorithmType
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithm, PeriodicMonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task


@pytest.fixture()
def create_task(sa_monitoring_algorithm_repo,
                sa_periodic_monitoring_algorithm_repo,
                sa_payload_repo,
                sa_task_repo):
    async def _create_task(task_id: int, status: TaskStatus, status_updated_at: datetime, timeout: float = 10):
        monitoring_algorithm = MonitoringAlgorithm(id=task_id + 1_000, type=MonitoringAlgorithmType.PERIODIC)
        periodic_monitoring_algorithm = PeriodicMonitoringAlgorithm(id=monitoring_algorithm.id,
                                                                    timeout=timeout)
        payload = Payload(id=task_id + 1_000, data={"username": "test_username"})
        task = Task(
            id=task_id,
            group_name="api_monitoring",
            priority=PriorityType.HIGH,
            type=TaskType.TIME_INTERVAL,
            monitoring_algorithm_id=monitoring_algorithm.id,
            status=status,
            status_updated_at=status_updated_at,
            payload_id=payload.id,
        )
        await sa_monitoring_algorithm_repo.create(monitoring_algorithm)
        await sa_periodic_monitoring_algorithm_repo.create(periodic_monitoring_algorithm)
        await sa_payload_repo.create(payload)
        await sa_task_repo.create(task)
        return task

    _create_task: Callable[[int, TaskStatus, datetime, float], Awaitable[Task]]
    return _create_task


@pytest_asyncio.fixture
async def _new_task_periodic_ma(create_task, ):
    target_datetime = datetime.now()
    task = await create_task(1, TaskStatus.NEW, target_datetime)
    return task


@pytest.fixture
def new_task_periodic_ma(_new_task_periodic_ma):
    _new_task_periodic_ma: Task
    return _new_task_periodic_ma


@pytest_asyncio.fixture
async def _succeed_task_periodic_ma_ready_to_execute(create_task, ):
    timeout = 30
    target_datetime = datetime.now() - timedelta(seconds=timeout + 1)
    task = await create_task(2, TaskStatus.SUCCEED, target_datetime, timeout)
    return task


@pytest.fixture
def succeed_task_periodic_ma_ready_to_execute(_succeed_task_periodic_ma_ready_to_execute, ):
    _succeed_task_periodic_ma_ready_to_execute: Task
    return _succeed_task_periodic_ma_ready_to_execute


@pytest_asyncio.fixture
async def _execution_task_periodic_ma_ready_to_execute(create_task, ):
    timeout = 30
    target_datetime = datetime.now() - timedelta(seconds=timeout + 1)
    task = await create_task(3, TaskStatus.EXECUTION, target_datetime, timeout)
    return task


@pytest.fixture
def execution_task_periodic_ma_ready_to_execute(_execution_task_periodic_ma_ready_to_execute, ):
    _execution_task_periodic_ma_ready_to_execute: Task
    return _execution_task_periodic_ma_ready_to_execute


@pytest_asyncio.fixture
async def _execution_task_periodic_ma_not_ready_to_execute(create_task, ):
    timeout = 30
    target_datetime = datetime.now()
    task = await create_task(4, TaskStatus.EXECUTION, target_datetime, timeout)
    return task


@pytest.fixture
def execution_task_periodic_ma_not_ready_to_execute(_execution_task_periodic_ma_not_ready_to_execute, ):
    _execution_task_periodic_ma_not_ready_to_execute: Task
    return _execution_task_periodic_ma_not_ready_to_execute


@pytest.mark.asyncio
async def test_provide_tasks_to_execute_new(sa_periodic_monitoring_algorithm_repo, new_task_periodic_ma):
    tasks = await sa_periodic_monitoring_algorithm_repo.provide_tasks_to_execute()
    assert tasks
    assert len(tasks) == 1
    task = tasks[0]
    assert task.id == new_task_periodic_ma.id


@pytest.mark.asyncio
async def test_provide_tasks_to_execute_succeed(sa_periodic_monitoring_algorithm_repo,
                                                succeed_task_periodic_ma_ready_to_execute):
    tasks = await sa_periodic_monitoring_algorithm_repo.provide_tasks_to_execute()
    assert tasks
    assert len(tasks) == 1
    task = tasks[0]
    assert task.id == succeed_task_periodic_ma_ready_to_execute.id


@pytest.mark.asyncio
async def test_provide_tasks_to_execute_execution(sa_periodic_monitoring_algorithm_repo,
                                                  execution_task_periodic_ma_ready_to_execute):
    tasks = await sa_periodic_monitoring_algorithm_repo.provide_tasks_to_execute()
    assert tasks
    assert len(tasks) == 1
    task = tasks[0]
    assert task.id == execution_task_periodic_ma_ready_to_execute.id


@pytest.mark.asyncio
async def test_provide_tasks_to_execute(sa_periodic_monitoring_algorithm_repo,
                                        new_task_periodic_ma,
                                        succeed_task_periodic_ma_ready_to_execute,
                                        execution_task_periodic_ma_ready_to_execute):
    tasks = await sa_periodic_monitoring_algorithm_repo.provide_tasks_to_execute()
    assert tasks
    assert len(tasks) == 3


@pytest.mark.asyncio
async def test_provide_tasks_not_ready_to_execute(sa_periodic_monitoring_algorithm_repo,
                                                  execution_task_periodic_ma_not_ready_to_execute):
    tasks = await sa_periodic_monitoring_algorithm_repo.provide_tasks_to_execute()
    assert not tasks

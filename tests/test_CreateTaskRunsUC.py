import pprint
from datetime import datetime, timedelta
from typing import Dict

import pytest

from service.domain.schemas.enums import TaskType, TaskStatus, MonitoringAlgorithmType, TaskRunStatus
from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithm, PeriodicMonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_progress import TimeIntervalTaskProgress
from service.domain.schemas.task_run import TaskRunTimeIntervalExecutionBounds, TaskRun
from service.domain.use_cases.internal.create_task_runs import CreateTaskRunsUCRq
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


@pytest.fixture
def create_payload(sa_payload_repo):
    async def _inner(data: Dict = None):
        data = data if data else {"username": "ivanov"}
        p = Payload(data=data)
        p_created = await sa_payload_repo.create(p)
        return p_created

    return _inner


@pytest.fixture
def create_periodic_monitoring_algorithm(sa_monitoring_algorithm_repo,
                                         sa_periodic_monitoring_algorithm_repo):
    async def _inner(timeout: int = 10):
        m = MonitoringAlgorithm(type=MonitoringAlgorithmType.PERIODIC)
        m_created = await sa_monitoring_algorithm_repo.create(m)
        pma = PeriodicMonitoringAlgorithm(id=m_created.id, timeout=timeout, )
        pma_created = await sa_periodic_monitoring_algorithm_repo.create(pma)
        return pma_created

    return _inner


@pytest.fixture
def create_task_v2(sa_task_repo, sa_task_group_repo):
    async def _inner(payload: Payload, monitoring_algorithm: MonitoringAlgorithm,
                     status_update_elapsed_seconds: int = 100,
                     status: TaskStatus = TaskStatus.NEW):
        group_name = "test"
        task_group = await sa_task_group_repo.create(TaskGroup(name=group_name, title='', description=''))
        t = Task(
            type=TaskType.TIME_INTERVAL,
            status=status,
            status_updated_at=datetime.now() - timedelta(seconds=status_update_elapsed_seconds),
            payload_id=payload.id,
            monitoring_algorithm_id=monitoring_algorithm.id,
            group_id=task_group.id,
        )
        t_created = await sa_task_repo.create(t)
        return t_created

    return _inner


@pytest.fixture
def create_time_interval_task_progress(sa_time_interval_task_progress_repo,
                                       sa_task_run_time_interval_execution_bounds_repo,
                                       sa_task_run_repo,
                                       sa_task_group_repo):
    async def _inner(task: Task, right_bound_at: datetime = None, left_bound_at: datetime = None, ):
        right_bound_at = right_bound_at or datetime.now() - timedelta(days=1)
        left_bound_at = left_bound_at if left_bound_at else datetime.min
        t = TimeIntervalTaskProgress(task_id=task.id, right_bound_at=right_bound_at, left_bound_at=left_bound_at,
                                     collected_data_amount=10, saved_data_amount=10)
        execution_bounds = TimeIntervalBounds(right_bound_at=t.right_bound_at,
                                              left_bound_at=t.left_bound_at)
        task_group = await sa_task_group_repo.get(TaskGroupPK(id=task.group_id))
        task_run = TaskRun(task_id=task.id, group_name=task_group.name, priority=task.priority, type=task.type,
                           payload=None, execution_bounds=execution_bounds, status=TaskRunStatus.SUCCEED,
                           status_updated_at=datetime.now())
        task_run = await sa_task_run_repo.create(task_run)
        await sa_task_run_time_interval_execution_bounds_repo.create(TaskRunTimeIntervalExecutionBounds(
            task_run_id=task_run.id, task_id=task.id, execution_bounds=execution_bounds
        ))
        t_created = await sa_time_interval_task_progress_repo.create(t)
        return t_created

    return _inner


@pytest.mark.asyncio
async def test_apply_one_task_two_runs(create_task_runs_uc, sa_task_run_repo, create_payload, create_task_v2,
                                       create_periodic_monitoring_algorithm, ):  # NEW -> EXECUTION: 2 WAITING RUNS
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm)
    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())
    assert response.task_runs_created == 8
    task_runs = await sa_task_run_repo.get_all()
    assert len(task_runs) == response.task_runs_created

    for task_run in task_runs:
        assert task_run.task_id == task.id


@pytest.mark.asyncio
async def test_apply_one_task_one_run(create_task_runs_uc, create_payload, create_task_v2,
                                      create_periodic_monitoring_algorithm, create_time_interval_task_progress,
                                      sa_task_run_repo):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.SUCCEED, )
    time_interval_task_progress = await create_time_interval_task_progress(task)
    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())
    assert response.task_runs_created == 1
    task_runs = await sa_task_run_repo.filter(FilterFieldsDNF.single('status', TaskRunStatus.SUCCEED, ConditionOperation.NE))

    assert len(task_runs) == response.task_runs_created
    for task_run in task_runs:
        assert task_run.task_id == task.id


@pytest.mark.asyncio
async def test_apply_one_task_null_run(create_task_runs_uc, create_payload, create_task_v2,
                                       create_periodic_monitoring_algorithm, create_time_interval_task_progress,
                                       sa_task_run_repo):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.EXECUTION,
                                status_update_elapsed_seconds=5)
    await create_time_interval_task_progress(task)
    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())
    assert response.task_runs_created == 0
    task_runs = await sa_task_run_repo.get_all()
    assert len(task_runs) == 1
    assert task_runs[0].status == TaskRunStatus.SUCCEED

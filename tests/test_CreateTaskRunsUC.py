from datetime import datetime, timezone, timedelta
from typing import Dict

import pytest
import pytest_asyncio

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
                     status: TaskStatus = TaskStatus.NEW,
                     task_type: TaskType = TaskType.TIME_INTERVAL,
                     time_interval_max_period: int = None,
                     time_interval_first_left_bound_depth: float = 86400,
                     ):
        group_name = "test"
        task_group = await sa_task_group_repo.create(
            TaskGroup(
                name=group_name, title='', description='',
                time_interval_max_period=time_interval_max_period,
                time_interval_first_left_bound_depth=time_interval_first_left_bound_depth
            )
        )
        t = Task(
            type=task_type,
            status=status,
            status_updated_at=datetime.now(timezone.utc) - timedelta(seconds=status_update_elapsed_seconds),
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
        right_bound_at = right_bound_at or datetime.now(timezone.utc) - timedelta(days=1)
        left_bound_at = left_bound_at if left_bound_at else datetime.min
        t = TimeIntervalTaskProgress(task_id=task.id, right_bound_at=right_bound_at, left_bound_at=left_bound_at,
                                     collected_data_amount=10, saved_data_amount=10)
        execution_bounds = TimeIntervalBounds(right_bound_at=t.right_bound_at,
                                              left_bound_at=t.left_bound_at)
        task_group = await sa_task_group_repo.get(TaskGroupPK(id=task.group_id))
        task_run = TaskRun(task_id=task.id, group_name=task_group.name, priority=task.priority, type=task.type,
                           payload=None, execution_bounds=execution_bounds, status=TaskRunStatus.SUCCEED,
                           status_updated_at=datetime.now(timezone.utc))
        task_run = await sa_task_run_repo.create(task_run)
        await sa_task_run_time_interval_execution_bounds_repo.create(TaskRunTimeIntervalExecutionBounds(
            task_run_id=task_run.id, task_id=task.id, execution_bounds=execution_bounds
        ))
        t_created = await sa_time_interval_task_progress_repo.create(t)
        return t_created

    return _inner


@pytest.mark.asyncio
async def test_apply_one_task_three_runs(create_task_runs_uc, sa_task_run_repo, create_payload, create_task_v2,
                                       create_periodic_monitoring_algorithm, ):  # NEW -> EXECUTION: 3 WAITING RUNS
    """ Ожидаемое поведение: мы указали максимальный интервал, запуск должен быть меньше максимального времени """
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, time_interval_max_period=43200, time_interval_first_left_bound_depth=100_000)
    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())
    assert response.task_runs_created == 3
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
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.SUCCEED,
                                time_interval_max_period=100_000,
                                time_interval_first_left_bound_depth=86400)
    time_interval_task_progress = await create_time_interval_task_progress(task)
    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())
    assert response.task_runs_created == 1
    task_runs = await sa_task_run_repo.filter(
        FilterFieldsDNF.single('status', TaskRunStatus.SUCCEED, ConditionOperation.NE))

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


@pytest.mark.asyncio
async def test_apply_creates_status_logs_and_time_interval_execution_bounds(
        create_task_runs_uc,
        create_payload,
        create_task_v2,
        create_periodic_monitoring_algorithm,
        sa_task_repo,
        sa_task_run_repo,
        sa_task_status_log_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm,
                                time_interval_max_period=100_000,
                                time_interval_first_left_bound_depth=86_400)

    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())

    assert response.task_runs_created == 1
    updated_task = await sa_task_repo.get(task)
    assert updated_task.status == TaskStatus.EXECUTION

    task_runs = await sa_task_run_repo.get_all()
    task_status_logs = await sa_task_status_log_repo.get_all()
    task_run_status_logs = await sa_task_run_status_log_repo.get_all()
    task_run_execution_bounds = await sa_task_run_time_interval_execution_bounds_repo.get_all()

    assert len(task_status_logs) == 1
    assert task_status_logs[0].task_id == task.id
    assert task_status_logs[0].status == TaskStatus.EXECUTION
    assert len(task_run_status_logs) == response.task_runs_created
    assert {log.status for log in task_run_status_logs} == {TaskRunStatus.WAITING}
    assert len(task_run_execution_bounds) == response.task_runs_created
    assert {bounds.task_run_id for bounds in task_run_execution_bounds} == {task_run.id for task_run in task_runs}
    assert all(
        bounds.execution_bounds.right_bound_at > bounds.execution_bounds.left_bound_at
        for bounds in task_run_execution_bounds
    )


@pytest.mark.asyncio
async def test_apply_creates_generic_run_for_pagination_task(
        create_task_runs_uc,
        create_payload,
        create_task_v2,
        create_periodic_monitoring_algorithm,
        sa_task_run_repo,
        sa_task_run_time_interval_execution_bounds_repo,
):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, task_type=TaskType.PAGINATION)

    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())

    assert response.task_runs_created == 1
    task_runs = await sa_task_run_repo.get_all()
    assert len(task_runs) == 1
    assert task_runs[0].task_id == task.id
    assert task_runs[0].type == TaskType.PAGINATION
    assert task_runs[0].execution_bounds is None
    assert await sa_task_run_time_interval_execution_bounds_repo.get_all() == []


@pytest.mark.asyncio
async def test_apply_uses_time_interval_progress_to_cut_overlapping_bounds(
        create_task_runs_uc,
        create_payload,
        create_task_v2,
        create_periodic_monitoring_algorithm,
        create_time_interval_task_progress,
        sa_task_run_repo,
):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.SUCCEED)
    progress_right_bound_at = datetime.now(timezone.utc) - timedelta(days=1)
    await create_time_interval_task_progress(task, right_bound_at=progress_right_bound_at)

    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())

    assert response.task_runs_created == 1
    task_runs = await sa_task_run_repo.filter(
        FilterFieldsDNF.single('status', TaskRunStatus.SUCCEED, ConditionOperation.NE))
    assert len(task_runs) == 1
    assert task_runs[0].execution_bounds.left_bound_at == progress_right_bound_at
    assert task_runs[0].execution_bounds.right_bound_at > progress_right_bound_at


@pytest.mark.asyncio
async def test_apply_skips_time_interval_run_when_latest_right_bound_is_not_less_than_now(
        create_task_runs_uc,
        create_payload,
        create_task_v2,
        create_periodic_monitoring_algorithm,
        create_time_interval_task_progress,
        sa_task_run_repo,
        sa_task_run_time_interval_execution_bounds_repo,
):
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.SUCCEED)
    await create_time_interval_task_progress(
        task,
        left_bound_at=datetime.now(timezone.utc),
        right_bound_at=datetime.now(timezone.utc) + timedelta(days=1),
    )

    response = await create_task_runs_uc.apply(CreateTaskRunsUCRq())

    assert response.task_runs_created == 0
    task_runs = await sa_task_run_repo.filter(
        FilterFieldsDNF.single('status', TaskRunStatus.SUCCEED, ConditionOperation.NE))
    assert task_runs == []
    task_run_execution_bounds = await sa_task_run_time_interval_execution_bounds_repo.get_all()
    assert all(
        bounds.execution_bounds.right_bound_at > bounds.execution_bounds.left_bound_at
        for bounds in task_run_execution_bounds
    )


@pytest_asyncio.fixture
async def ch_client():
    import os

    import clickhouse_connect

    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    client = await clickhouse_connect.get_async_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
    yield client
    await client.close()


@pytest_asyncio.fixture
async def ch_time_interval_repos(ch_client):
    from service.adapters.outbound.repo.ch.impls.task_run_time_interval_execution_bounds import (
        CHTaskRunTimeIntervalExecutionBoundsRepo,
    )
    from service.adapters.outbound.repo.ch.impls.time_interval_task_progress import CHTimeIntervalTaskProgressRepo

    bounds_repo = CHTaskRunTimeIntervalExecutionBoundsRepo(ch_client)
    progress_repo = CHTimeIntervalTaskProgressRepo(ch_client)
    await ch_client.command(f"DROP TABLE IF EXISTS {bounds_repo.table_name}")
    await ch_client.command(f"DROP TABLE IF EXISTS {progress_repo.table_name}")
    await bounds_repo.create_table_if_not_exists()
    await progress_repo.create_table_if_not_exists()
    yield bounds_repo, progress_repo
    await ch_client.command(f"DROP TABLE IF EXISTS {bounds_repo.table_name}")
    await ch_client.command(f"DROP TABLE IF EXISTS {progress_repo.table_name}")


@pytest_asyncio.fixture
async def create_task_runs_uc_with_clickhouse_time_interval_tables(
        sa_task_repo,
        sa_task_run_repo,
        sa_task_status_log_repo,
        sa_task_run_status_log_repo,
        sa_transaction_factory,
        task_to_execute_provider_registry,
        payload_provider,
        sa_task_group_repo,
        ch_time_interval_repos,
):
    from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider
    from service.domain.services.task_progress_provider import ActualTimeIntervalExecutionBoundsProvider
    from service.domain.use_cases.internal.create_task_runs import CreateTaskRunsUC

    ch_bounds_repo, ch_progress_repo = ch_time_interval_repos
    return CreateTaskRunsUC(
        sa_task_repo,
        sa_task_run_repo,
        sa_task_status_log_repo,
        sa_task_run_status_log_repo,
        ch_bounds_repo,
        sa_transaction_factory,
        task_to_execute_provider_registry,
        DefaultExecutionBoundsProvider(ch_bounds_repo),
        payload_provider,
        ActualTimeIntervalExecutionBoundsProvider(ch_progress_repo),
        sa_task_group_repo,
    )

@pytest.mark.skip  # FIXME: test method when clickhouse is not working
@pytest.mark.asyncio
async def test_apply_can_use_real_clickhouse_time_interval_tables(
        create_task_runs_uc_with_clickhouse_time_interval_tables,
        create_payload,
        create_task_v2,
        create_periodic_monitoring_algorithm,
        ch_time_interval_repos,
        sa_task_run_repo,
):
    ch_bounds_repo, ch_progress_repo = ch_time_interval_repos
    payload = await create_payload()
    monitoring_algorithm = await create_periodic_monitoring_algorithm()
    task = await create_task_v2(payload, monitoring_algorithm, status=TaskStatus.SUCCEED)
    previous_right_bound_at = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0)
    previous_bounds = TimeIntervalBounds(
        left_bound_at=datetime(2020, 1, 1),
        right_bound_at=previous_right_bound_at,
    )
    await ch_bounds_repo.create(TaskRunTimeIntervalExecutionBounds(
        task_run_id=1,
        task_id=task.id,
        execution_bounds=previous_bounds,
    ))
    await ch_progress_repo.create(TimeIntervalTaskProgress(
        task_id=task.id,
        left_bound_at=previous_bounds.left_bound_at,
        right_bound_at=previous_bounds.right_bound_at,
        collected_data_amount=10,
        saved_data_amount=10,
    ))

    response = await create_task_runs_uc_with_clickhouse_time_interval_tables.apply(CreateTaskRunsUCRq())

    assert response.task_runs_created == 1
    created_task_runs = await sa_task_run_repo.get_all()
    assert len(created_task_runs) == 1
    assert created_task_runs[0].execution_bounds.left_bound_at == previous_right_bound_at
    assert await ch_bounds_repo.get_latest_right_bound_by_task_ids([task.id])

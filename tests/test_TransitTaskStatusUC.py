from datetime import datetime

import pytest
import pytest_asyncio

from service.domain.schemas.enums import (
    TaskStatus, TaskRunStatus, TaskType
)
from service.domain.schemas.monitoring_algorithm import PeriodicMonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_run import TaskRun
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq
from service.domain.use_cases.internal.transit_task_status import (
    TransitTaskStatusUC,
    TransitTaskStatusUCRq,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------



@pytest_asyncio.fixture
async def _monitoring_algorithm_id(create_monitoring_algorithm_uc) -> int:
    algorithm = PeriodicMonitoringAlgorithm(timeout=3600.0, timeout_noize=0.0)
    response = await create_monitoring_algorithm_uc.apply(
        CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    )
    return response.created_algorithm.id


@pytest.fixture
def monitoring_algorithm_id(_monitoring_algorithm_id):
    _monitoring_algorithm_id: int
    return _monitoring_algorithm_id


async def _create_task(
        sa_task_repo,
        sa_payload_repo,
        monitoring_algorithm_id: int,
        status: TaskStatus = TaskStatus.EXECUTION,
        group_name: str = "test_group",
) -> Task:
    payload = await sa_payload_repo.create(
        Payload(data={"key": group_name})
    )
    task = Task(
        type=TaskType.TIME_INTERVAL,
        status=status,
        status_updated_at=datetime.utcnow(),
        payload_id=payload.id,
        monitoring_algorithm_id=monitoring_algorithm_id,
        group_name=group_name,
    )
    return await sa_task_repo.create(task)


async def _create_task_run(
        sa_task_run_repo,
        task_id: int,
        status: TaskRunStatus,
        status_updated_at: datetime = None,
) -> TaskRun:
    task_run = TaskRun(
        task_id=task_id,
        status=status,
        status_updated_at=status_updated_at or datetime.utcnow(),
        execution_arguments={},
        group_name='test'
    )
    return await sa_task_run_repo.create(task_run)


# ---------------------------------------------------------------------------
# 1. No EXECUTION tasks — nothing to do
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_execution_tasks(transit_task_status_uc):
    request = TransitTaskStatusUCRq()
    response = await transit_task_status_uc.apply(request)

    assert response.success is True
    assert response.succeed_count == 0
    assert response.error_count == 0


# ---------------------------------------------------------------------------
# 2. EXECUTION task with all SUCCEED runs → transitions to SUCCEED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_transitions_to_succeed_when_all_runs_succeed(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.success is True
    assert response.succeed_count == 1
    assert response.error_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.SUCCEED


# ---------------------------------------------------------------------------
# 3. EXECUTION task with all ERROR runs → transitions to ERROR
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_transitions_to_error_when_all_runs_error(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR)

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.success is True
    assert response.succeed_count == 0
    assert response.error_count == 1

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.ERROR


# ---------------------------------------------------------------------------
# 4. EXECUTION task with at least one SUCCEED in last 3 runs → SUCCEED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_transitions_to_succeed_when_one_of_three_runs_succeeds(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    t = datetime(2024, 1, 1, 0, 0, 0)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR,
                           status_updated_at=t)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR,
                           status_updated_at=t.replace(second=1))
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED,
                           status_updated_at=t.replace(second=2))  # Latest

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 1
    assert response.error_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.SUCCEED


# ---------------------------------------------------------------------------
# 5. EXECUTION task with RUNNING task run → not completed, not transitioned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_not_transitioned_when_has_running_task_run(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    # Mix of finished and still-running
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.EXECUTION)  # Not finished

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 0
    assert response.error_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.EXECUTION  # Unchanged


# ---------------------------------------------------------------------------
# 6. EXECUTION task with QUEUED task run → not completed, not transitioned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_not_transitioned_when_has_queued_task_run(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.QUEUED)  # Pending

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.EXECUTION


# ---------------------------------------------------------------------------
# 7. Task with no task runs at all → not transitioned (continue path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_not_transitioned_when_no_task_runs(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)
    # No task runs created

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 0
    assert response.error_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.EXECUTION


# ---------------------------------------------------------------------------
# 8. Only LAST 3 runs are analyzed — old SUCCEED before 3 errors is ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_last_3_runs_are_analyzed(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    t = datetime(2024, 1, 1, 0, 0, 0)
    # Old SUCCEED run — should be outside the window of last 3
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED,
                           status_updated_at=t)
    # 3 recent ERROR runs
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR,
                           status_updated_at=t.replace(second=1))
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR,
                           status_updated_at=t.replace(second=2))
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR,
                           status_updated_at=t.replace(second=3))

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    # Only last 3 (all ERROR) should be analyzed → ERROR
    assert response.error_count == 1
    assert response.succeed_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.ERROR


# ---------------------------------------------------------------------------
# 9. Non-EXECUTION tasks are not affected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_execution_tasks_not_affected(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    # Tasks in other statuses
    new_task = await _create_task(
        sa_task_repo, sa_payload_repo, monitoring_algorithm_id,
        status=TaskStatus.NEW, group_name="new_task"
    )
    succeed_task = await _create_task(
        sa_task_repo, sa_payload_repo, monitoring_algorithm_id,
        status=TaskStatus.SUCCEED, group_name="succeed_task"
    )
    error_task = await _create_task(
        sa_task_repo, sa_payload_repo, monitoring_algorithm_id,
        status=TaskStatus.ERROR, group_name="error_task"
    )

    # Add task runs for each
    for task in [new_task, succeed_task, error_task]:
        await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.ERROR)

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 0
    assert response.error_count == 0

    # Statuses unchanged
    assert (await sa_task_repo.get(TaskPK(id=new_task.id))).status == TaskStatus.NEW
    assert (await sa_task_repo.get(TaskPK(id=succeed_task.id))).status == TaskStatus.SUCCEED
    assert (await sa_task_repo.get(TaskPK(id=error_task.id))).status == TaskStatus.ERROR


# ---------------------------------------------------------------------------
# 10. Multiple tasks — each transitions independently
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_tasks_transition_independently(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    succeed_task = await _create_task(
        sa_task_repo, sa_payload_repo, monitoring_algorithm_id, group_name="will_succeed"
    )
    error_task = await _create_task(
        sa_task_repo, sa_payload_repo, monitoring_algorithm_id, group_name="will_error"
    )

    await _create_task_run(sa_task_run_repo, succeed_task.id, TaskRunStatus.SUCCEED)
    await _create_task_run(sa_task_run_repo, succeed_task.id, TaskRunStatus.SUCCEED)

    await _create_task_run(sa_task_run_repo, error_task.id, TaskRunStatus.ERROR)
    await _create_task_run(sa_task_run_repo, error_task.id, TaskRunStatus.ERROR)

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    assert response.succeed_count == 1
    assert response.error_count == 1

    assert (await sa_task_repo.get(TaskPK(id=succeed_task.id))).status == TaskStatus.SUCCEED
    assert (await sa_task_repo.get(TaskPK(id=error_task.id))).status == TaskStatus.ERROR


# ---------------------------------------------------------------------------
# 11. Status log is created for transitioned tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_log_created_for_transitioned_task(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_task_status_log_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.SUCCEED)

    await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    from service.ports.outbound.repo.fields import FilterFieldsDNF
    logs = await sa_task_status_log_repo.filter(
        FilterFieldsDNF.single("task_id", task.id)
    )

    assert len(logs) >= 1
    latest_log = max(logs, key=lambda l: l.status_updated_at)
    assert latest_log.status == TaskStatus.SUCCEED
    assert latest_log.task_id == task.id


# ---------------------------------------------------------------------------
# 12. Task with INTERRUPTED runs is considered not completed → stays EXECUTION
# INTERRUPTED means the run was lost in queue or the worker crashed.
# Another component will later move it to WAITING for retry.
# Since INTERRUPTED is NOT IN (SUCCEED, ERROR), the task is not yet done.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_with_interrupted_runs_stays_in_execution(
        transit_task_status_uc,
        sa_task_repo,
        sa_task_run_repo,
        sa_payload_repo,
        monitoring_algorithm_id,
):
    """
    INTERRUPTED попадает под NOT_IN (SUCCEED, ERROR) → задача считается незавершённой.
    Другой компонент переведёт INTERRUPTED → WAITING для повторного запуска.
    TransitTaskStatusUC не трогает такие задачи.
    """
    task = await _create_task(sa_task_repo, sa_payload_repo, monitoring_algorithm_id)

    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.INTERRUPTED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.INTERRUPTED)
    await _create_task_run(sa_task_run_repo, task.id, TaskRunStatus.INTERRUPTED)

    response = await transit_task_status_uc.apply(TransitTaskStatusUCRq())

    # Ожидаемое поведение: задача ещё не завершена, ждёт перезапуска
    assert response.succeed_count == 0
    assert response.error_count == 0

    updated_task = await sa_task_repo.get(TaskPK(id=task.id))
    assert updated_task.status == TaskStatus.EXECUTION


# ---------------------------------------------------------------------------
# 13. Response always has success=True and contains request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_response_contains_request_and_success(transit_task_status_uc):
    request = TransitTaskStatusUCRq()
    response = await transit_task_status_uc.apply(request)

    assert response.success is True
    assert response.request is request

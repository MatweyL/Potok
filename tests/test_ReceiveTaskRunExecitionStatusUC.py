from datetime import datetime
from typing import Optional

import pytest
import pytest_asyncio

from service.domain.schemas.command import Command, CommandResponse
from service.domain.schemas.enums import CommandType, TaskRunStatus, TaskType, TaskStatus, PriorityType, \
    MonitoringAlgorithmType
from service.domain.schemas.execution_results import TimeIntervalExecutionResults
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.domain.schemas.task_group import TaskGroup
from service.domain.schemas.task_run import TaskRun, TaskRunPK
from service.domain.use_cases.internal.receive_task_run_execution_status import (
    ReceiveTaskRunExecutionStatusUC,
    ReceiveTaskRunExecutionStatusUCRq,
)


# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def default_task_group(sa_task_group_repo):
    task_group = await sa_task_group_repo.create(TaskGroup(id=1, name='test', title='', description=''))
    return task_group
@pytest_asyncio.fixture
async def default_monitoring_algorithm(sa_monitoring_algorithm_repo):
    monitoring_algorithm = await sa_monitoring_algorithm_repo.create(MonitoringAlgorithm(id=1, type=MonitoringAlgorithmType.PERIODIC))
    return monitoring_algorithm
@pytest_asyncio.fixture
async def default_payload(sa_payload_repo):
    payload = await sa_payload_repo.create(Payload(id=1, data={}))
    return payload

@pytest_asyncio.fixture
async def default_task(sa_task_repo):
    task = _make_task()
    task = await sa_task_repo.create(task)
    return task


def _make_task(task_id: int = 1, group_id: int = 1) -> Task:
    """Create a minimal Task stub."""
    return Task(
        id=task_id,
        type=TaskType.TIME_INTERVAL,
        status=TaskStatus.NEW,
        status_updated_at=datetime(2024, 1, 1),
        payload_id=1,
        monitoring_algorithm_id=1,
        group_id=group_id,
        priority=PriorityType.MEDIUM,
    )


def _make_task_run(
        task_run_id: int = 10,
        task_id: int = 1,
        status: TaskRunStatus = TaskRunStatus.WAITING,
) -> TaskRun:
    """Create a minimal TaskRun stub."""
    return TaskRun(
        id=task_run_id,
        task_id=task_id,
        status=status,
        status_updated_at=datetime(2024, 6, 1),
        execution_arguments={},
        group_name='test'
    )


def _make_command(task_run: TaskRun, command_type: CommandType = CommandType.EXECUTE) -> Command:
    """Create a Command."""
    return Command(type=command_type, task_run=task_run)


def _make_command_response(
        command: Command,
        status: TaskRunStatus,
        description: Optional[str] = None,
        result: Optional[TimeIntervalExecutionResults] = None,
        created_at: Optional[datetime] = None,
) -> CommandResponse:
    """Create a CommandResponse."""
    return CommandResponse(
        command=command,
        status=status,
        description=description,
        result=result,
        created_at=created_at or datetime(2024, 6, 15, 12, 0, 0),
    )


@pytest.fixture
def receive_task_run_execution_status_uc(
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_time_interval_task_progress_repo,
        sa_transaction_factory,
) -> ReceiveTaskRunExecutionStatusUC:
    return ReceiveTaskRunExecutionStatusUC(
        task_run_repo=sa_task_run_repo,
        task_run_status_log_repo=sa_task_run_status_log_repo,
        time_interval_task_progress_repo=sa_time_interval_task_progress_repo,
        transaction_factory=sa_transaction_factory,
    )


# ---------------------------------------------------------------------------
# 1. Basic success flow — status update without result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_updates_task_run_status_without_result(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_time_interval_task_progress_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    # Создаём task_run в БД
    task_run = _make_task_run(task_run_id=10,)
    created_task_run = await sa_task_run_repo.create(task_run)

    command = _make_command(created_task_run)
    created_at = datetime(2024, 6, 15, 12, 0, 0)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        description="Task is now EXECUTION",
        result=None,
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await receive_task_run_execution_status_uc.apply(request)

    # Проверяем что task_run обновился в БД
    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=10))
    assert updated_task_run.status == TaskRunStatus.EXECUTION
    assert updated_task_run.status_updated_at == created_at

    # Проверяем что создался status log
    from service.ports.outbound.repo.fields import FilterFieldsDNF
    status_logs = await sa_task_run_status_log_repo.filter(
        FilterFieldsDNF.single("task_run_id", 10)
    )
    assert len(status_logs) == 1
    assert status_logs[0].status == TaskRunStatus.EXECUTION
    assert status_logs[0].description == "Task is now EXECUTION"

    # Проверяем что прогресс НЕ создался (нет result)
    progress_records = await sa_time_interval_task_progress_repo.filter(
        FilterFieldsDNF.single("task_id", 1)
    )
    assert len(progress_records) == 0

    # Проверяем response
    assert response.success is True
    assert response.request == request


# ---------------------------------------------------------------------------
# 2. Success flow — with execution result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_progress_when_result_present(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_time_interval_task_progress_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    # Создаём task_run в БД
    task_run = _make_task_run(task_run_id=20,)
    created_task_run = await sa_task_run_repo.create(task_run)

    command = _make_command(created_task_run)
    created_at = datetime(2024, 6, 15, 14, 30, 0)

    execution_result = TimeIntervalExecutionResults(
        right_bound_at=datetime(2024, 6, 15),
        left_bound_at=datetime(2024, 5, 1),
        collected_data_amount=100,
        saved_data_amount=95,
    )

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        description="Completed successfully",
        result=execution_result,
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await receive_task_run_execution_status_uc.apply(request)

    # Проверяем обновление task_run
    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=20))
    assert updated_task_run.status == TaskRunStatus.SUCCEED

    # Проверяем что создался прогресс
    from service.ports.outbound.repo.fields import FilterFieldsDNF
    progress_records = await sa_time_interval_task_progress_repo.filter(
        FilterFieldsDNF.single("task_id", 1)
    )
    assert len(progress_records) == 1
    progress = progress_records[0]
    assert progress.task_id == 1
    assert progress.right_bound_at == datetime(2024, 6, 15)
    assert progress.left_bound_at == datetime(2024, 5, 1)
    assert progress.collected_data_amount == 100
    assert progress.saved_data_amount == 95

    assert response.success is True


# ---------------------------------------------------------------------------
# 3. Different statuses — ERROR
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_ERROR_status(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=30,)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)
    created_at = datetime(2024, 6, 15, 15, 0, 0)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.ERROR,
        description="Execution failed",
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    # Проверяем статус
    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=30))
    assert updated_task_run.status == TaskRunStatus.ERROR

    # Проверяем status log
    from service.ports.outbound.repo.fields import FilterFieldsDNF
    status_logs = await sa_task_run_status_log_repo.filter(
        FilterFieldsDNF.single("task_run_id", 30)
    )
    assert len(status_logs) == 1
    assert status_logs[0].status == TaskRunStatus.ERROR
    assert status_logs[0].description == "Execution failed"


# ---------------------------------------------------------------------------
# 4. Different statuses — INTERRUPTED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_INTERRUPTED_status(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=40)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.INTERRUPTED,
        description="Task was interrupted",
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=40))
    assert updated_task_run.status == TaskRunStatus.INTERRUPTED


# ---------------------------------------------------------------------------
# 5. Result with large amounts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_large_amounts_in_result(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_time_interval_task_progress_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=50, )
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    execution_result = TimeIntervalExecutionResults(
        right_bound_at=datetime(2024, 6, 15),
        left_bound_at=datetime(2024, 6, 1),
        collected_data_amount=200,
        saved_data_amount=180,
    )

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        result=execution_result,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    from service.ports.outbound.repo.fields import FilterFieldsDNF
    progress_records = await sa_time_interval_task_progress_repo.filter(
        FilterFieldsDNF.single("task_id", 1)
    )
    assert len(progress_records) == 1
    assert progress_records[0].collected_data_amount == 200
    assert progress_records[0].saved_data_amount == 180


# ---------------------------------------------------------------------------
# 6. Description is optional (None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_none_description(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=60)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        description=None,  # Explicitly None
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    from service.ports.outbound.repo.fields import FilterFieldsDNF
    status_logs = await sa_task_run_status_log_repo.filter(
        FilterFieldsDNF.single("task_run_id", 60)
    )
    assert len(status_logs) == 1
    assert status_logs[0].description is None


# ---------------------------------------------------------------------------
# 7. Multiple calls — sequential status updates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_status_updates(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=70)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    # First update: EXECUTION
    cr1 = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        created_at=datetime(2024, 6, 15, 10, 0, 0),
    )
    await receive_task_run_execution_status_uc.apply(
        ReceiveTaskRunExecutionStatusUCRq(command_response=cr1)
    )

    # Second update: SUCCEED
    cr2 = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        created_at=datetime(2024, 6, 15, 10, 30, 0),
    )
    await receive_task_run_execution_status_uc.apply(
        ReceiveTaskRunExecutionStatusUCRq(command_response=cr2)
    )

    # Проверяем финальный статус
    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=70))
    assert updated_task_run.status == TaskRunStatus.SUCCEED

    # Проверяем что создались 2 status log
    from service.ports.outbound.repo.fields import FilterFieldsDNF
    status_logs = await sa_task_run_status_log_repo.filter(
        FilterFieldsDNF.single("task_run_id", 70)
    )
    assert len(status_logs) == 2


# ---------------------------------------------------------------------------
# 8. Result with zero collected/saved amounts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_zero_amounts_in_result(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_time_interval_task_progress_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=80, )
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    execution_result = TimeIntervalExecutionResults(
        right_bound_at=datetime(2024, 6, 15),
        left_bound_at=datetime(2024, 6, 1),
        collected_data_amount=0,
        saved_data_amount=0,
    )

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        result=execution_result,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    from service.ports.outbound.repo.fields import FilterFieldsDNF
    progress_records = await sa_time_interval_task_progress_repo.filter(
        FilterFieldsDNF.single("task_id", 1)
    )
    assert len(progress_records) == 1
    assert progress_records[0].collected_data_amount == 0
    assert progress_records[0].saved_data_amount == 0


# ---------------------------------------------------------------------------
# 9. Verify status_updated_at is taken from command_response.created_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_updated_at_uses_command_response_created_at(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=90)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)
    specific_time = datetime(2024, 7, 1, 8, 45, 30)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        created_at=specific_time,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await receive_task_run_execution_status_uc.apply(request)

    # Проверяем task_run
    updated_task_run = await sa_task_run_repo.get(TaskRunPK(id=90))
    assert updated_task_run.status_updated_at == specific_time

    # Проверяем status log
    from service.ports.outbound.repo.fields import FilterFieldsDNF
    status_logs = await sa_task_run_status_log_repo.filter(
        FilterFieldsDNF.single("task_run_id", 90)
    )
    assert status_logs[0].status_updated_at == specific_time


# ---------------------------------------------------------------------------
# 10. Response always returns success=True
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_response_always_success_true(
        receive_task_run_execution_status_uc,
        sa_task_run_repo,
        sa_task_repo,
        sa_task_group_repo,
        default_task_group,
        default_monitoring_algorithm,
        default_payload,
        default_task,
):
    task_run = _make_task_run(task_run_id=100,)
    await sa_task_run_repo.create(task_run)

    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await receive_task_run_execution_status_uc.apply(request)

    assert response.success is True
    assert response.request is request

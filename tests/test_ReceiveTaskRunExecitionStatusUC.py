from datetime import datetime
from typing import Optional
from unittest.mock import AsyncMock

import pytest

from service.domain.schemas.command import Command, CommandResponse
from service.domain.schemas.enums import CommandType, TaskRunStatus, TaskType, TaskStatus
from service.domain.schemas.execution_results import TimeIntervalExecutionResults
from service.domain.schemas.task import Task
from service.domain.schemas.task_progress import TimeIntervalTaskProgress
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunStatusLog
from service.domain.use_cases.internal.receive_task_run_execution_status import (
    ReceiveTaskRunExecutionStatusUC,
    ReceiveTaskRunExecutionStatusUCRq,
)
from service.ports.outbound.repo.fields import UpdateFields


# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------


def _make_task(task_id: int = 1) -> Task:
    """Create a minimal Task stub."""
    return Task(
        id=task_id,
        type=TaskType.TIME_INTERVAL,
        status=TaskStatus.NEW,
        status_updated_at=datetime(2024, 1, 1),
        payload_id=1,
        monitoring_algorithm_id=1,
        group_name="test_group",
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
def mock_task_run_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.update = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_task_run_status_log_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_time_interval_task_progress_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_transaction_factory() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def use_case(
    mock_task_run_repo,
    mock_task_run_status_log_repo,
    mock_time_interval_task_progress_repo,
    mock_transaction_factory,
) -> ReceiveTaskRunExecutionStatusUC:
    return ReceiveTaskRunExecutionStatusUC(
        task_run_repo=mock_task_run_repo,
        task_run_status_log_repo=mock_task_run_status_log_repo,
        time_interval_task_progress_repo=mock_time_interval_task_progress_repo,
        transaction_factory=mock_transaction_factory,
    )


# ---------------------------------------------------------------------------
# 1. Basic success flow — status update without result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_updates_task_run_status_without_result(
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
    mock_time_interval_task_progress_repo,
):
    task_run = _make_task_run(task_run_id=10, task_id=1)
    command = _make_command(task_run)
    created_at = datetime(2024, 6, 15, 12, 0, 0)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        description="Task is now EXECUTION",
        result=None,
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await use_case.apply(request)

    # Verify task_run was updated
    mock_task_run_repo.update.assert_awaited_once()
    call_args = mock_task_run_repo.update.call_args
    assert call_args[0][0] == TaskRunPK(id=10)
    update_fields: UpdateFields = call_args[0][1]
    assert update_fields.to_dict() == {
        "status": TaskRunStatus.EXECUTION,
        "status_updated_at": created_at,
    }

    # Verify status log was created
    mock_task_run_status_log_repo.create.assert_awaited_once()
    status_log: TaskRunStatusLog = mock_task_run_status_log_repo.create.call_args[0][0]
    assert status_log.task_run_id == 10
    assert status_log.status == TaskRunStatus.EXECUTION
    assert status_log.status_updated_at == created_at
    assert status_log.description == "Task is now EXECUTION"

    # Verify progress was NOT created (no result)
    mock_time_interval_task_progress_repo.create.assert_not_awaited()

    # Verify response
    assert response.success is True
    assert response.request == request


# ---------------------------------------------------------------------------
# 2. Success flow — with execution result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_progress_when_result_present(
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
    mock_time_interval_task_progress_repo,
):
    task_run = _make_task_run(task_run_id=20, task_id=5)
    command = _make_command(task_run)
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
    response = await use_case.apply(request)

    # Verify task_run was updated
    mock_task_run_repo.update.assert_awaited_once()
    update_pk, update_fields = mock_task_run_repo.update.call_args[0]
    assert update_pk == TaskRunPK(id=20)
    assert update_fields.to_dict()["status"] == TaskRunStatus.SUCCEED

    # Verify status log was created
    mock_task_run_status_log_repo.create.assert_awaited_once()

    # Verify progress WAS created
    mock_time_interval_task_progress_repo.create.assert_awaited_once()
    progress: TimeIntervalTaskProgress = mock_time_interval_task_progress_repo.create.call_args[0][0]
    assert progress.task_id == 5
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
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
):
    task_run = _make_task_run(task_run_id=30, task_id=7)
    command = _make_command(task_run)
    created_at = datetime(2024, 6, 15, 15, 0, 0)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.ERROR,
        description="Error: Connection timeout",
        result=None,
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await use_case.apply(request)

    # Verify status was updated to ERROR
    update_fields: UpdateFields = mock_task_run_repo.update.call_args[0][1]
    assert update_fields.to_dict()["status"] == TaskRunStatus.ERROR

    # Verify log contains error description
    status_log: TaskRunStatusLog = mock_task_run_status_log_repo.create.call_args[0][0]
    assert status_log.status == TaskRunStatus.ERROR
    assert status_log.description == "Error: Connection timeout"

    assert response.success is True


# ---------------------------------------------------------------------------
# 4. Different statuses — CANCELLED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_cancelled_status(
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
):
    task_run = _make_task_run(task_run_id=40)
    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.CANCELLED,
        description="User requested cancellation",
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    update_fields: UpdateFields = mock_task_run_repo.update.call_args[0][1]
    assert update_fields.to_dict()["status"] == TaskRunStatus.CANCELLED


# ---------------------------------------------------------------------------
# 5. Progress with partial save (collected != saved)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_progress_with_partial_save(
    use_case,
    mock_time_interval_task_progress_repo,
):
    task_run = _make_task_run(task_run_id=50, task_id=8)
    command = _make_command(task_run)

    execution_result = TimeIntervalExecutionResults(
        right_bound_at=datetime(2024, 6, 10),
        left_bound_at=datetime(2024, 6, 1),
        collected_data_amount=200,
        saved_data_amount=180,  # Partial save
    )

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        result=execution_result,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    progress: TimeIntervalTaskProgress = mock_time_interval_task_progress_repo.create.call_args[0][0]
    assert progress.collected_data_amount == 200
    assert progress.saved_data_amount == 180


# ---------------------------------------------------------------------------
# 6. Description is optional (None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_none_description(
    use_case,
    mock_task_run_status_log_repo,
):
    task_run = _make_task_run(task_run_id=60)
    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        description=None,  # Explicitly None
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    status_log: TaskRunStatusLog = mock_task_run_status_log_repo.create.call_args[0][0]
    assert status_log.description is None


# ---------------------------------------------------------------------------
# 7. Multiple calls — repos called correctly each time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_status_updates(
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
):
    task_run = _make_task_run(task_run_id=70)
    command = _make_command(task_run)

    # First update: EXECUTION
    cr1 = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        created_at=datetime(2024, 6, 15, 10, 0, 0),
    )
    await use_case.apply(ReceiveTaskRunExecutionStatusUCRq(command_response=cr1))

    # Second update: SUCCEED
    cr2 = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
        created_at=datetime(2024, 6, 15, 10, 30, 0),
    )
    await use_case.apply(ReceiveTaskRunExecutionStatusUCRq(command_response=cr2))

    # Repos should have been called twice
    assert mock_task_run_repo.update.await_count == 2
    assert mock_task_run_status_log_repo.create.await_count == 2


# ---------------------------------------------------------------------------
# 8. Result with zero collected/saved amounts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handles_zero_amounts_in_result(
    use_case,
    mock_time_interval_task_progress_repo,
):
    task_run = _make_task_run(task_run_id=80, task_id=9)
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
    await use_case.apply(request)

    progress: TimeIntervalTaskProgress = mock_time_interval_task_progress_repo.create.call_args[0][0]
    assert progress.collected_data_amount == 0
    assert progress.saved_data_amount == 0


# ---------------------------------------------------------------------------
# 9. Verify status_updated_at is taken from command_response.created_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_updated_at_uses_command_response_created_at(
    use_case,
    mock_task_run_repo,
    mock_task_run_status_log_repo,
):
    task_run = _make_task_run(task_run_id=90)
    command = _make_command(task_run)
    specific_time = datetime(2024, 7, 1, 8, 45, 30)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
        created_at=specific_time,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    # Check task_run update
    update_fields: UpdateFields = mock_task_run_repo.update.call_args[0][1]
    assert update_fields.to_dict()["status_updated_at"] == specific_time

    # Check status log
    status_log: TaskRunStatusLog = mock_task_run_status_log_repo.create.call_args[0][0]
    assert status_log.status_updated_at == specific_time


# ---------------------------------------------------------------------------
# 11. Verify TaskRunPK is constructed correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_run_pk_constructed_correctly(
    use_case,
    mock_task_run_repo,
):
    task_run = _make_task_run(task_run_id=12345)
    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.EXECUTION,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    pk_arg = mock_task_run_repo.update.call_args[0][0]
    assert isinstance(pk_arg, TaskRunPK)
    assert pk_arg.id == 12345


# ---------------------------------------------------------------------------
# 12. Verify all three repos are injected correctly
# ---------------------------------------------------------------------------


def test_use_case_initialization():
    mock_task_run = AsyncMock()
    mock_status_log = AsyncMock()
    mock_progress = AsyncMock()
    mock_tf = AsyncMock()

    uc = ReceiveTaskRunExecutionStatusUC(
        task_run_repo=mock_task_run,
        task_run_status_log_repo=mock_status_log,
        time_interval_task_progress_repo=mock_progress,
        transaction_factory=mock_tf,
    )

    assert uc._task_run_repo is mock_task_run
    assert uc._task_run_status_log_repo is mock_status_log
    assert uc._time_interval_task_progress_repo is mock_progress
    assert uc._transaction_factory is mock_tf


# ---------------------------------------------------------------------------
# 13. Response always returns success=True (no error handling in UC)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_response_always_success_true(use_case):
    task_run = _make_task_run()
    command = _make_command(task_run)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.SUCCEED,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    response = await use_case.apply(request)

    assert response.success is True
    assert response.request is request


# ---------------------------------------------------------------------------
# 14. Verify UpdateFields.multiple is used correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_fields_multiple_usage(
    use_case,
    mock_task_run_repo,
):
    task_run = _make_task_run(task_run_id=999)
    command = _make_command(task_run)
    created_at = datetime(2024, 8, 1, 12, 0, 0)

    command_response = _make_command_response(
        command=command,
        status=TaskRunStatus.CANCELLED,
        created_at=created_at,
    )

    request = ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)
    await use_case.apply(request)

    update_fields: UpdateFields = mock_task_run_repo.update.call_args[0][1]
    fields_dict = update_fields.to_dict()

    # Verify it contains exactly the two fields
    assert len(fields_dict) == 2
    assert fields_dict["status"] == TaskRunStatus.CANCELLED
    assert fields_dict["status_updated_at"] == created_at

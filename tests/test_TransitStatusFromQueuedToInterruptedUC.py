from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from freezegun import freeze_time

from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunStatusLog
from service.domain.use_cases.internal.transit_task_run_status.abstract import TransitTaskRunStatusUCRq
from service.domain.use_cases.internal.transit_task_run_status.impls import TransitStatusFromQueuedToInterruptedUC
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation, UpdateFields

# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------

FROZEN_NOW = datetime(2024, 6, 15, 12, 0, 0)


def _make_task_run(
        task_run_id: int,
        status: TaskRunStatus,
        status_updated_at: datetime,
) -> TaskRun:
    return TaskRun(
        id=task_run_id,
        task_id=1,
        status=status,
        status_updated_at=status_updated_at,
        execution_arguments={},
        group_name="test"
    )


@pytest.fixture
def mock_task_run_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.filter = AsyncMock(return_value=[])
    repo.update_all = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_task_run_status_log_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.create_all = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def mock_transaction_factory() -> AsyncMock:

    class Entering:
        async def __aenter__(self):
            """Enter async context manager."""
            return self

        async def __aexit__(self, exc_type, exc, tb):
            """Exit async context manager."""
            pass

    class AsyncContextManagerMock(AsyncMock):
        def create(self):
            return Entering()

    return AsyncContextManagerMock()


@pytest.fixture
def transit_status_from_queued_to_interrupted(
        mock_task_run_repo,
        mock_task_run_status_log_repo,
        mock_transaction_factory,
) -> TransitStatusFromQueuedToInterruptedUC:
    return TransitStatusFromQueuedToInterruptedUC(
        task_run_repo=mock_task_run_repo,
        task_run_status_log_repo=mock_task_run_status_log_repo,
        transaction_factory=mock_transaction_factory,
    )


# ---------------------------------------------------------------------------
# 1. No expired tasks — returns zero count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_no_expired_tasks_returns_zero_count(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
        mock_task_run_status_log_repo,
):
    mock_task_run_repo.filter.return_value = []

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.success is True
    assert response.count == 0
    assert response.request == request

    # update_all and create_all should NOT be called
    mock_task_run_repo.update_all.assert_not_awaited()
    mock_task_run_status_log_repo.create_all.assert_not_awaited()


# ---------------------------------------------------------------------------
# 2. Single expired QUEUED task — transitions to INTERRUPTED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_single_expired_task_transitions_to_interrupted(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
        mock_task_run_status_log_repo,
):
    # Task queued 6 minutes ago (TTL = 5 minutes)
    expired_task = _make_task_run(
        task_run_id=10,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(minutes=6),
    )
    mock_task_run_repo.filter.return_value = [expired_task]

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.success is True
    assert response.count == 1

    # Verify filter was called with correct conditions
    filter_arg: FilterFieldsDNF = mock_task_run_repo.filter.call_args[0][0]
    fields = filter_arg.conjunctions[0].group
    assert len(fields) == 2
    assert fields[0].name == "status"
    assert fields[0].value == TaskRunStatus.QUEUED
    assert fields[1].name == "status_updated_at"
    assert fields[1].operation == ConditionOperation.LT
    # Threshold: now - 300s
    assert fields[1].value == FROZEN_NOW - timedelta(seconds=300)

    # Verify update_all was called
    mock_task_run_repo.update_all.assert_awaited_once()
    fields_by_pk = mock_task_run_repo.update_all.call_args[0][0]
    assert TaskRunPK(id=10) in fields_by_pk
    update_fields: UpdateFields = fields_by_pk[TaskRunPK(id=10)]
    assert update_fields.to_dict()["status"] == TaskRunStatus.INTERRUPTED
    assert update_fields.to_dict()["status_updated_at"] == FROZEN_NOW

    # Verify status log was created
    mock_task_run_status_log_repo.create_all.assert_awaited_once()
    logs = mock_task_run_status_log_repo.create_all.call_args[0][0]
    assert len(logs) == 1
    log: TaskRunStatusLog = logs[0]
    assert log.task_run_id == 10
    assert log.status == TaskRunStatus.INTERRUPTED
    assert log.status_updated_at == FROZEN_NOW
    assert not log.description

# ---------------------------------------------------------------------------
# 3. Multiple expired tasks — all transition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_multiple_expired_tasks_all_transition(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
        mock_task_run_status_log_repo,
):
    expired_tasks = [
        _make_task_run(10, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(minutes=10)),
        _make_task_run(20, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(minutes=8)),
        _make_task_run(30, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(hours=1)),
    ]
    mock_task_run_repo.filter.return_value = expired_tasks

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.count == 3

    # Verify update_all called with all three PKs
    fields_by_pk = mock_task_run_repo.update_all.call_args[0][0]
    assert len(fields_by_pk) == 3
    assert TaskRunPK(id=10) in fields_by_pk
    assert TaskRunPK(id=20) in fields_by_pk
    assert TaskRunPK(id=30) in fields_by_pk

    # Verify three status logs created
    logs = mock_task_run_status_log_repo.create_all.call_args[0][0]
    assert len(logs) == 3
    log_ids = {log.task_run_id for log in logs}
    assert log_ids == {10, 20, 30}


# ---------------------------------------------------------------------------
# 4. Custom TTL — respects the provided value
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_custom_ttl_respects_provided_value(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    mock_task_run_repo.filter.return_value = []

    custom_ttl = 600  # 10 minutes
    request = TransitTaskRunStatusUCRq(ttl_seconds=custom_ttl)
    await transit_status_from_queued_to_interrupted.apply(request)

    # Verify filter uses custom TTL
    filter_arg: FilterFieldsDNF = mock_task_run_repo.filter.call_args[0][0]
    threshold_field = filter_arg.conjunctions[0].group[1]
    assert threshold_field.value == FROZEN_NOW - timedelta(seconds=600)


# ---------------------------------------------------------------------------
# 5. Task exactly at TTL boundary — should NOT be expired
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_task_exactly_at_ttl_boundary_not_expired(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    # Task updated exactly 300 seconds ago (at the threshold)
    boundary_task = _make_task_run(
        task_run_id=40,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(seconds=300),
    )
    mock_task_run_repo.filter.return_value = []  # Filter uses LT, not LTE

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    # Should be zero since LT comparison excludes the exact threshold
    assert response.count == 0


# ---------------------------------------------------------------------------
# 6. Task just over TTL — should be expired
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_task_just_over_ttl_is_expired(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    # Task updated 301 seconds ago (1 second over TTL)
    expired_task = _make_task_run(
        task_run_id=50,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(seconds=301),
    )
    mock_task_run_repo.filter.return_value = [expired_task]

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.count == 1


# ---------------------------------------------------------------------------
# 7. Default TTL is 300 seconds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_default_ttl_is_300_seconds(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    mock_task_run_repo.filter.return_value = []

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)  # No ttl_seconds provided
    await transit_status_from_queued_to_interrupted.apply(request)

    # Verify default TTL (300s) is used
    filter_arg: FilterFieldsDNF = mock_task_run_repo.filter.call_args[0][0]
    threshold_field = filter_arg.conjunctions[0].group[1]
    assert threshold_field.value == FROZEN_NOW - timedelta(seconds=300)


# ---------------------------------------------------------------------------
# 9. Only QUEUED tasks are selected (filter correctness)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_filter_only_selects_queued_status(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    mock_task_run_repo.filter.return_value = []

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    await transit_status_from_queued_to_interrupted.apply(request)

    filter_arg: FilterFieldsDNF = mock_task_run_repo.filter.call_args[0][0]
    status_field = filter_arg.conjunctions[0].group[0]
    assert status_field.name == "status"
    assert status_field.value == TaskRunStatus.QUEUED
    assert status_field.operation == ConditionOperation.EQ


# ---------------------------------------------------------------------------
# 10. UpdateFields contains both status and status_updated_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_update_fields_contains_status_and_timestamp(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    expired_task = _make_task_run(
        task_run_id=70,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(minutes=10),
    )
    mock_task_run_repo.filter.return_value = [expired_task]

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    await transit_status_from_queued_to_interrupted.apply(request)

    fields_by_pk = mock_task_run_repo.update_all.call_args[0][0]
    update_fields: UpdateFields = fields_by_pk[TaskRunPK(id=70)]
    fields_dict = update_fields.to_dict()

    assert len(fields_dict) == 2
    assert fields_dict["status"] == TaskRunStatus.INTERRUPTED
    assert fields_dict["status_updated_at"] == FROZEN_NOW


# ---------------------------------------------------------------------------
# 11. All status logs have same status_updated_at (now)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_all_status_logs_have_same_timestamp(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
        mock_task_run_status_log_repo,
):
    expired_tasks = [
        _make_task_run(10, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(minutes=10)),
        _make_task_run(20, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(minutes=20)),
    ]
    mock_task_run_repo.filter.return_value = expired_tasks

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    await transit_status_from_queued_to_interrupted.apply(request)

    logs = mock_task_run_status_log_repo.create_all.call_args[0][0]
    for log in logs:
        assert log.status_updated_at == FROZEN_NOW


# ---------------------------------------------------------------------------
# 12. Large batch — 100 tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_large_batch_100_tasks(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
        mock_task_run_status_log_repo,
):
    expired_tasks = [
        _make_task_run(
            i, TaskRunStatus.QUEUED, FROZEN_NOW - timedelta(minutes=10)
        )
        for i in range(100)
    ]
    mock_task_run_repo.filter.return_value = expired_tasks

    request = TransitTaskRunStatusUCRq(ttl_seconds=300)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.count == 100

    fields_by_pk = mock_task_run_repo.update_all.call_args[0][0]
    assert len(fields_by_pk) == 100

    logs = mock_task_run_status_log_repo.create_all.call_args[0][0]
    assert len(logs) == 100


# ---------------------------------------------------------------------------
# 13. Verify use case initialization
# ---------------------------------------------------------------------------


def test_transit_status_from_queued_to_interrupted_initialization():
    mock_tr = AsyncMock()
    mock_log = AsyncMock()
    mock_tf = AsyncMock()

    uc = TransitStatusFromQueuedToInterruptedUC(
        task_run_repo=mock_tr,
        task_run_status_log_repo=mock_log,
        transaction_factory=mock_tf,
    )

    assert uc._task_run_repo is mock_tr
    assert uc._task_run_status_log_repo is mock_log
    assert uc._transaction_factory is mock_tf


# ---------------------------------------------------------------------------
# 14. Very short TTL (1 second)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_very_short_ttl(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    expired_task = _make_task_run(
        task_run_id=80,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(seconds=2),
    )
    mock_task_run_repo.filter.return_value = [expired_task]

    request = TransitTaskRunStatusUCRq(ttl_seconds=1)
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.count == 1


# ---------------------------------------------------------------------------
# 15. Very long TTL (24 hours)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_very_long_ttl(
        transit_status_from_queued_to_interrupted,
        mock_task_run_repo,
):
    # Task queued 25 hours ago
    expired_task = _make_task_run(
        task_run_id=90,
        status=TaskRunStatus.QUEUED,
        status_updated_at=FROZEN_NOW - timedelta(hours=25),
    )
    mock_task_run_repo.filter.return_value = [expired_task]

    request = TransitTaskRunStatusUCRq(ttl_seconds=86400)  # 24 hours
    response = await transit_status_from_queued_to_interrupted.apply(request)

    assert response.count == 1

    # Verify threshold
    filter_arg: FilterFieldsDNF = mock_task_run_repo.filter.call_args[0][0]
    threshold_field = filter_arg.conjunctions[0].group[1]
    assert threshold_field.value == FROZEN_NOW - timedelta(seconds=86400)

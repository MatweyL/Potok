from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from freezegun import freeze_time

from service.domain.schemas.enums import TaskType, TaskStatus
from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task import Task
from service.domain.schemas.task_progress import (
    TimeIntervalTaskProgress,
)
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation
from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

FROZEN_NOW = datetime(2024, 6, 15, 12, 0, 0)


def _make_task(task_id: int, task_type: TaskType = TaskType.TIME_INTERVAL) -> Task:
    return Task(
        id=task_id,
        type=task_type,
        status=TaskStatus.NEW,
        status_updated_at=datetime.now(),
        payload_id=1,
        monitoring_algorithm_id=1,
        group_name="test",
    )


def _make_progress(
    task_id: int,
    right_bound_at: datetime,
    left_bound_at: datetime,
    collected: int = 10,
    saved: int = 10,
) -> TimeIntervalTaskProgress:
    return TimeIntervalTaskProgress(
        task_id=task_id,
        right_bound_at=right_bound_at,
        left_bound_at=left_bound_at,
        collected_data_amount=collected,
        saved_data_amount=saved,
    )


@pytest.fixture()
def mock_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.filter = AsyncMock(return_value=[])
    return repo


@pytest.fixture()
def provider(mock_repo: AsyncMock) -> DefaultExecutionBoundsProvider:
    return DefaultExecutionBoundsProvider(
        time_interval_progress_repo=mock_repo,
        default_left_date=datetime(2010, 1, 1),
        default_first_interval_days=31,
    )


# ---------------------------------------------------------------------------
# 1. Empty input
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_task_list_returns_empty_dict(provider, mock_repo):
    result = await provider.provide_batch([])

    mock_repo.filter.assert_not_called()
    assert result == {}


# ---------------------------------------------------------------------------
# 2. Single TIME_INTERVAL task — NO progress (first run)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_no_progress_returns_two_bounds(provider, mock_repo):
    mock_repo.filter.return_value = []

    task = _make_task(1)
    result = await provider.provide_batch([task])

    assert task in result
    bounds = result[task]
    assert len(bounds) == 2

    # First bound: [now - 31d, now] — monitoring interval
    assert bounds[0].right_bound_at == FROZEN_NOW
    assert bounds[0].left_bound_at == FROZEN_NOW - timedelta(days=31)

    # Second bound: [default_left_date, now - 31d] — retro interval
    assert bounds[1].right_bound_at == FROZEN_NOW - timedelta(days=31)
    assert bounds[1].left_bound_at == datetime(2010, 1, 1)


# ---------------------------------------------------------------------------
# 3. Single TIME_INTERVAL task — progress exists & is COMPLETE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_completed_progress_returns_single_bound_from_last_right(provider, mock_repo):
    last_right = datetime(2024, 6, 1, 0, 0, 0)
    mock_repo.filter.return_value = [
        _make_progress(
            task_id=1,
            right_bound_at=last_right,
            left_bound_at=datetime(2024, 5, 1),
            collected=5,
            saved=5,
        ),
    ]

    task = _make_task(1)
    result = await provider.provide_batch([task])

    bounds = result[task]
    assert len(bounds) == 1
    assert bounds[0].left_bound_at == last_right
    assert bounds[0].right_bound_at == FROZEN_NOW


# ---------------------------------------------------------------------------
# 4. Single TIME_INTERVAL task — progress exists but is INCOMPLETE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_incomplete_progress_still_returns_bound_from_last_right(provider, mock_repo):
    last_right = datetime(2024, 6, 1, 0, 0, 0)
    mock_repo.filter.return_value = [
        _make_progress(
            task_id=1,
            right_bound_at=last_right,
            left_bound_at=datetime(2024, 5, 1),
            collected=10,
            saved=3,  # incomplete
        ),
    ]

    task = _make_task(1)
    result = await provider.provide_batch([task])

    bounds = result[task]
    assert len(bounds) == 1
    # Текущий код не различает — left всё равно last right_bound_at
    assert bounds[0].left_bound_at == last_right
    assert bounds[0].right_bound_at == FROZEN_NOW


# ---------------------------------------------------------------------------
# 5. Multiple progress records — picks the one with max right_bound_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_multiple_progress_records_picks_latest(provider, mock_repo):
    old_right = datetime(2024, 5, 1)
    new_right = datetime(2024, 6, 10)

    mock_repo.filter.return_value = [
        _make_progress(
            task_id=1,
            right_bound_at=old_right,
            left_bound_at=datetime(2024, 4, 1),
            collected=5,
            saved=5,
        ),
        _make_progress(
            task_id=1,
            right_bound_at=new_right,
            left_bound_at=old_right,
            collected=8,
            saved=8,
        ),
    ]

    task = _make_task(1)
    result = await provider.provide_batch([task])

    bounds = result[task]
    assert len(bounds) == 1
    assert bounds[0].left_bound_at == new_right
    assert bounds[0].right_bound_at == FROZEN_NOW


# ---------------------------------------------------------------------------
# 6. Batch: mix of tasks with and without progress
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_batch_mixed_progress(provider, mock_repo):
    task_with_progress = _make_task(1)
    task_without_progress = _make_task(2)

    last_right = datetime(2024, 6, 1)
    mock_repo.filter.return_value = [
        _make_progress(
            task_id=1,
            right_bound_at=last_right,
            left_bound_at=datetime(2024, 5, 1),
            collected=4,
            saved=4,
        ),
    ]

    result = await provider.provide_batch([task_with_progress, task_without_progress])

    # Task 1 — single bound continuing from last_right
    assert len(result[task_with_progress]) == 1
    assert result[task_with_progress][0].left_bound_at == last_right
    assert result[task_with_progress][0].right_bound_at == FROZEN_NOW

    # Task 2 — two bounds (first-run scenario)
    assert len(result[task_without_progress]) == 2


# ---------------------------------------------------------------------------
# 7. Unsupported task type gets an empty list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsupported_task_type_returns_empty_bounds(provider, mock_repo):
    task = _make_task(99, task_type=TaskType.PAGINATION)

    result = await provider.provide_batch([task])

    assert result[task] == []
    mock_repo.filter.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Repo filter is called with correct task_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_repo_filter_called_with_correct_task_ids(provider, mock_repo):
    mock_repo.filter.return_value = []

    tasks = [_make_task(10), _make_task(20), _make_task(30)]
    await provider.provide_batch(tasks)

    mock_repo.filter.assert_awaited_once()
    call_args = mock_repo.filter.call_args[0][0]

    assert isinstance(call_args, FilterFieldsDNF)
    field = call_args.conjunctions[0].group[0]
    assert field.name == "task_id"
    assert field.operation == ConditionOperation.IN
    assert set(field.value) == {10, 20, 30}


# ---------------------------------------------------------------------------
# 9. All returned bounds are TimeIntervalBounds instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_all_bounds_are_time_interval_bounds(provider, mock_repo):
    mock_repo.filter.return_value = []

    task = _make_task(1)
    result = await provider.provide_batch([task])

    for bound in result[task]:
        assert isinstance(bound, TimeIntervalBounds)
        assert bound.type == TaskType.TIME_INTERVAL


# ---------------------------------------------------------------------------
# 10. Custom default_left_date and default_first_interval_days are respected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_custom_config_values_are_respected(mock_repo):
    mock_repo.filter.return_value = []

    custom_left = datetime(2000, 3, 15)
    custom_days = 60

    provider = DefaultExecutionBoundsProvider(
        time_interval_progress_repo=mock_repo,
        default_left_date=custom_left,
        default_first_interval_days=custom_days,
    )

    task = _make_task(1)
    result = await provider.provide_batch([task])

    bounds = result[task]
    assert len(bounds) == 2

    assert bounds[0].right_bound_at == FROZEN_NOW
    assert bounds[0].left_bound_at == FROZEN_NOW - timedelta(days=60)

    assert bounds[1].right_bound_at == FROZEN_NOW - timedelta(days=60)
    assert bounds[1].left_bound_at == custom_left


# ---------------------------------------------------------------------------
# 11. default_left_date defaults to 2010-01-01 when not provided
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_default_left_date_fallback(mock_repo):
    mock_repo.filter.return_value = []

    provider = DefaultExecutionBoundsProvider(
        time_interval_progress_repo=mock_repo,
        default_left_date=None,
        default_first_interval_days=31,
    )

    task = _make_task(1)
    result = await provider.provide_batch([task])

    retro_bound = result[task][1]
    assert retro_bound.left_bound_at == datetime(2010, 1, 1)


# ---------------------------------------------------------------------------
# 12. Large batch — all tasks are accounted for in result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_large_batch_all_tasks_present_in_result(provider, mock_repo):
    mock_repo.filter.return_value = []

    tasks = [_make_task(i) for i in range(100)]
    result = await provider.provide_batch(tasks)

    assert len(result) == 100
    for task in tasks:
        assert task in result
        assert len(result[task]) == 2


# ---------------------------------------------------------------------------
# 13. Progress record where right_bound_at == now (edge: zero-width next interval)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@freeze_time(FROZEN_NOW)
async def test_progress_right_bound_equals_now_gives_zero_width_interval(provider, mock_repo):
    mock_repo.filter.return_value = [
        _make_progress(
            task_id=1,
            right_bound_at=FROZEN_NOW,
            left_bound_at=datetime(2024, 5, 15),
            collected=3,
            saved=3,
        ),
    ]

    task = _make_task(1)
    result = await provider.provide_batch([task])

    bounds = result[task]
    assert len(bounds) == 1
    # left == right → zero-width interval
    assert bounds[0].left_bound_at == FROZEN_NOW
    assert bounds[0].right_bound_at == FROZEN_NOW


# ---------------------------------------------------------------------------
# 14. Protocol compliance
# ---------------------------------------------------------------------------


def test_provider_satisfies_protocol(provider):
    from service.domain.services.execution_bounds_provider import ExecutionBoundsProvider

    assert isinstance(provider, ExecutionBoundsProvider)

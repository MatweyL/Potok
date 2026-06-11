from datetime import timedelta

import pytest

from service.domain.schemas.enums import TaskStatus
from service.domain.schemas.task_progress import TimeIntervalTaskProgress
from service.domain.use_cases.internal.compress_task_progress import CompressTaskProgressUCRq
from .conftest import sa_task_group_repo, sa_monitoring_algorithm_repo, sa_payload_repo
from .utils import make_utc_datetime, create_tasks

pytestmark = pytest.mark.asyncio


async def _create_progress(repo, items: list[TimeIntervalTaskProgress]):
    return await repo.create_all(items)


async def test_merges_records_within_max_gap(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo,
):
    """
    Три последовательных записи с разрывами <= max_gap должны слиться
    в одну с min(left_bound_at) / max(right_bound_at) и суммой счётчиков.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)

    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 1, TaskStatus.NEW)
    assert len(tasks) == 1
    task = tasks[0]
    await _create_progress(sa_time_interval_task_progress_repo, [
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # gap = 70:00 - 60:00 = 10s <= 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=70),
            right_bound_at=base + timedelta(seconds=130),
            collected_data_amount=20,
            saved_data_amount=15,
        ),
        # gap = 140:00 - 130:00 = 10s <= 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=140),
            right_bound_at=base + timedelta(seconds=200),
            collected_data_amount=5,
            saved_data_amount=5,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.success is True
    assert rs.tasks_processed == 1
    assert rs.records_before == 3
    assert rs.records_after == 1
    assert rs.records_removed == 2

    remaining = await sa_time_interval_task_progress_repo.get_all()
    assert len(remaining) == 1

    merged = remaining[0]
    assert merged.task_id == task.id
    assert merged.left_bound_at == base
    assert merged.right_bound_at == base + timedelta(seconds=200)
    assert merged.collected_data_amount == 35  # 10 + 20 + 5
    assert merged.saved_data_amount == 30  # 10 + 15 + 5


async def test_does_not_merge_records_with_large_gap(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Если разрыв между записями больше max_gap — записи не сливаются,
    остаются отдельными.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)

    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo, "test", 1, TaskStatus.NEW)
    assert len(tasks) == 1
    task = tasks[0]
    await _create_progress(sa_time_interval_task_progress_repo, [
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # gap = 200 - 60 = 140s > 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=200),
            right_bound_at=base + timedelta(seconds=260),
            collected_data_amount=20,
            saved_data_amount=20,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.success is True
    assert rs.tasks_processed == 0
    assert rs.records_before == 2
    assert rs.records_after == 2
    assert rs.records_removed == 0

    remaining = await sa_time_interval_task_progress_repo.get_all()
    assert len(remaining) == 2


async def test_partial_merge_creates_two_groups(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Записи: [A, B] сливаются (маленький gap), [B, C] не сливаются (большой gap).
    Итог: 2 записи — объединённая (A+B) и отдельная C.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)


    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 1, TaskStatus.NEW)
    assert len(tasks) == 1
    task = tasks[0]
    await _create_progress(sa_time_interval_task_progress_repo, [
        # A
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # B: gap A->B = 10s <= 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=70),
            right_bound_at=base + timedelta(seconds=130),
            collected_data_amount=20,
            saved_data_amount=20,
        ),
        # C: gap B->C = 200s > 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=330),
            right_bound_at=base + timedelta(seconds=400),
            collected_data_amount=5,
            saved_data_amount=5,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.records_before == 3
    assert rs.records_after == 2
    assert rs.records_removed == 1
    assert rs.tasks_processed == 1

    remaining = await sa_time_interval_task_progress_repo.get_all()
    remaining_sorted = sorted(remaining, key=lambda r: r.right_bound_at)
    assert len(remaining_sorted) == 2

    merged_ab = remaining_sorted[0]
    assert merged_ab.left_bound_at == base
    assert merged_ab.right_bound_at == base + timedelta(seconds=130)
    assert merged_ab.collected_data_amount == 30
    assert merged_ab.saved_data_amount == 30

    untouched_c = remaining_sorted[1]
    assert untouched_c.left_bound_at == base + timedelta(seconds=330)
    assert untouched_c.right_bound_at == base + timedelta(seconds=400)
    assert untouched_c.collected_data_amount == 5
    assert untouched_c.saved_data_amount == 5


async def test_multiple_tasks_processed_independently(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Записи разных задач не должны сливаться друг с другом,
    каждая задача обрабатывается независимо.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)
    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 2, TaskStatus.NEW)
    assert len(tasks) == 2
    task1 = tasks[0]
    task2 = tasks[1]

    await _create_progress(sa_time_interval_task_progress_repo, [
        # task_id=1 — две записи с маленьким gap, сольются
        TimeIntervalTaskProgress(
            task_id=task1.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        TimeIntervalTaskProgress(
            task_id=task1.id,
            left_bound_at=base + timedelta(seconds=65),
            right_bound_at=base + timedelta(seconds=120),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # task_id=2 — одна запись, нечего сливать
        TimeIntervalTaskProgress(
            task_id=task2.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=5,
            saved_data_amount=5,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.tasks_processed == 1
    assert rs.records_before == 3
    assert rs.records_after == 2
    assert rs.records_removed == 1

    remaining = await sa_time_interval_task_progress_repo.get_all()
    by_task = {}
    for r in remaining:
        by_task.setdefault(r.task_id, []).append(r)

    assert len(by_task[1]) == 1
    assert by_task[1][0].collected_data_amount == 20
    assert by_task[1][0].left_bound_at == base
    assert by_task[1][0].right_bound_at == base + timedelta(seconds=120)

    assert len(by_task[2]) == 1
    assert by_task[2][0].collected_data_amount == 5


async def test_records_without_left_bound_are_not_merged(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Записи с left_bound_at=None не участвуют в слиянии и остаются как есть.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)
    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 1, TaskStatus.NEW)
    assert len(tasks) == 1
    task = tasks[0]
    await _create_progress(sa_time_interval_task_progress_repo, [
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=None,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=65),
            right_bound_at=base + timedelta(seconds=120),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.records_before == 2
    assert rs.records_after == 2
    assert rs.records_removed == 0

    remaining = await sa_time_interval_task_progress_repo.get_all()
    assert len(remaining) == 2


async def test_filters_by_task_ids(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Если передан task_ids — обрабатываются только указанные задачи,
    остальные остаются нетронутыми даже если их можно было бы сжать.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)
    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 2, TaskStatus.NEW)
    assert len(tasks) == 2
    task1 = tasks[0]
    task2 = tasks[1]
    await _create_progress(sa_time_interval_task_progress_repo, [
        # task_id=1 — подходит для слияния, но не входит в task_ids
        TimeIntervalTaskProgress(
            task_id=task1.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        TimeIntervalTaskProgress(
            task_id=task1.id,
            left_bound_at=base + timedelta(seconds=65),
            right_bound_at=base + timedelta(seconds=120),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # task_id=2 — подходит для слияния и входит в task_ids
        TimeIntervalTaskProgress(
            task_id=task2.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=5,
            saved_data_amount=5,
        ),
        TimeIntervalTaskProgress(
            task_id=task2.id,
            left_bound_at=base + timedelta(seconds=65),
            right_bound_at=base + timedelta(seconds=120),
            collected_data_amount=5,
            saved_data_amount=5,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30, task_ids=[2])
    )

    assert rs.tasks_processed == 1
    assert rs.records_before == 2  # только записи task_id=2 загружены
    assert rs.records_after == 1
    assert rs.records_removed == 1

    remaining = await sa_time_interval_task_progress_repo.get_all()
    by_task = {}
    for r in remaining:
        by_task.setdefault(r.task_id, []).append(r)

    # task_id=1 нетронут — обе записи на месте
    assert len(by_task[1]) == 2

    # task_id=2 сжат в одну
    assert len(by_task[2]) == 1
    assert by_task[2][0].collected_data_amount == 10


async def test_no_records_returns_zero_stats(
        compress_task_progress_uc,
):
    """Пустая таблица — UC должен корректно завершиться без ошибок."""
    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.success is True
    assert rs.tasks_processed == 0
    assert rs.records_before == 0
    assert rs.records_after == 0
    assert rs.records_removed == 0


async def test_gap_exactly_equal_to_max_gap_is_merged(
        compress_task_progress_uc,
        sa_time_interval_task_progress_repo,
sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo,
):
    """
    Граничный случай: gap == max_gap_seconds (включительно) -> сливаются.
    """
    base = make_utc_datetime(2026, 6, 1, 0, 0, 0)
    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                       sa_payload_repo, "test", 1, TaskStatus.NEW)
    assert len(tasks) == 1
    task = tasks[0]
    await _create_progress(sa_time_interval_task_progress_repo, [
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base,
            right_bound_at=base + timedelta(seconds=60),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
        # gap = ровно 30s
        TimeIntervalTaskProgress(
            task_id=task.id,
            left_bound_at=base + timedelta(seconds=90),
            right_bound_at=base + timedelta(seconds=150),
            collected_data_amount=10,
            saved_data_amount=10,
        ),
    ])

    rs = await compress_task_progress_uc.apply(
        CompressTaskProgressUCRq(max_gap_seconds=30)
    )

    assert rs.records_after == 1
    remaining = await sa_time_interval_task_progress_repo.get_all()
    assert len(remaining) == 1
    assert remaining[0].right_bound_at == base + timedelta(seconds=150)

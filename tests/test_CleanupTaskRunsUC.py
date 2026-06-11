from datetime import timedelta

import pytest

from service.domain.schemas.enums import TaskRunStatus, TaskStatus
from service.domain.use_cases.internal.cleanup_task_runs import CleanupTaskRunsUCRq
from .utils import (
    create_tasks,
    create_task_run_with_children,
)

pytestmark = pytest.mark.asyncio


async def test_deletes_only_old_terminal_runs(
        cleanup_task_runs_uc,
        sa_task_repo,
        sa_task_group_repo,
        sa_monitoring_algorithm_repo,
        sa_payload_repo,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
        sa_task_run_time_interval_progress_repo,
):
    tasks = await create_tasks(
        sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
        sa_payload_repo, group_name="g1", tasks_amount=6, task_status=TaskStatus.SUCCEED,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    # 1: SUCCEED, 40 дней назад -> удалить
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[0].id, group_name="g1",
        status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=40),
    )

    # 2: ERROR, 35 дней назад -> удалить
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[1].id, group_name="g1",
        status=TaskRunStatus.ERROR, status_updated_at=now - timedelta(days=35),
    )

    # 3: CANCELLED, 31 день назад -> удалить
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[2].id, group_name="g1",
        status=TaskRunStatus.CANCELLED, status_updated_at=now - timedelta(days=31),
    )

    # 4: SUCCEED, 10 дней назад -> оставить (свежая)
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[3].id, group_name="g1",
        status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=10),
    )

    # 5: EXECUTION, 60 дней назад -> оставить (не терминальный статус)
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[4].id, group_name="g1",
        status=TaskRunStatus.EXECUTION, status_updated_at=now - timedelta(days=60),
    )

    # 6: WAITING, 100 дней назад -> оставить (не терминальный статус)
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[5].id, group_name="g1",
        status=TaskRunStatus.WAITING, status_updated_at=now - timedelta(days=100),
    )

    rs = await cleanup_task_runs_uc.apply(CleanupTaskRunsUCRq())

    assert rs.success is True
    assert rs.deleted_count == 3

    remaining = await sa_task_run_repo.get_all()
    remaining_statuses = {r.status for r in remaining}
    assert len(remaining) == 3
    assert remaining_statuses == {TaskRunStatus.SUCCEED, TaskRunStatus.EXECUTION, TaskRunStatus.WAITING}


async def test_cascades_to_child_tables(
        cleanup_task_runs_uc,
        sa_task_repo,
        sa_task_group_repo,
        sa_monitoring_algorithm_repo,
        sa_payload_repo,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
        sa_task_run_time_interval_progress_repo,
):
    tasks = await create_tasks(
        sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
        sa_payload_repo, group_name="g2", tasks_amount=2, task_status=TaskStatus.SUCCEED,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    # Запись для удаления — со всеми дочерними записями
    old_run = await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[0].id, group_name="g2",
        status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=40),
    )

    # Запись, которая остаётся — со своими дочерними записями
    fresh_run = await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[1].id, group_name="g2",
        status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=5),
    )

    rs = await cleanup_task_runs_uc.apply(CleanupTaskRunsUCRq())
    assert rs.deleted_count == 1

    # task_run
    remaining_runs = await sa_task_run_repo.get_all()
    assert {r.id for r in remaining_runs} == {fresh_run.id}

    # task_run_status_log
    remaining_logs = await sa_task_run_status_log_repo.get_all()
    assert all(log.task_run_id != old_run.id for log in remaining_logs)
    assert any(log.task_run_id == fresh_run.id for log in remaining_logs)

    # task_run_time_interval_execution_bounds
    remaining_bounds = await sa_task_run_time_interval_execution_bounds_repo.get_all()
    assert all(b.task_run_id != old_run.id for b in remaining_bounds)
    assert any(b.task_run_id == fresh_run.id for b in remaining_bounds)

    # task_run_time_interval_progress
    remaining_progress = await sa_task_run_time_interval_progress_repo.get_all()
    assert all(p.task_run_id != old_run.id for p in remaining_progress)
    assert any(p.task_run_id == fresh_run.id for p in remaining_progress)


async def test_batching_processes_all_records(
        cleanup_task_runs_uc,
        sa_task_repo,
        sa_task_group_repo,
        sa_monitoring_algorithm_repo,
        sa_payload_repo,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
        sa_task_run_time_interval_progress_repo,
):
    tasks = await create_tasks(
        sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
        sa_payload_repo, group_name="g3", tasks_amount=25, task_status=TaskStatus.SUCCEED,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    for task in tasks:
        await create_task_run_with_children(
            sa_task_run_repo, sa_task_run_status_log_repo,
            sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
            task_id=task.id, group_name="g3",
            status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=40),
            with_children=False,
        )

    # Маленький batch_size, чтобы проверить несколько итераций
    cleanup_task_runs_uc._batch_size = 10

    rs = await cleanup_task_runs_uc.apply(CleanupTaskRunsUCRq())

    assert rs.deleted_count == 25
    remaining = await sa_task_run_repo.get_all()
    assert len(remaining) == 0


async def test_no_candidates_does_nothing(
        cleanup_task_runs_uc,
        sa_task_repo,
        sa_task_group_repo,
        sa_monitoring_algorithm_repo,
        sa_payload_repo,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
        sa_task_run_time_interval_progress_repo,
):
    tasks = await create_tasks(
        sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
        sa_payload_repo, group_name="g4", tasks_amount=2, task_status=TaskStatus.SUCCEED,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    # Свежая SUCCEED — не подходит по retention
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[0].id, group_name="g4",
        status=TaskRunStatus.SUCCEED, status_updated_at=now - timedelta(days=5),
        with_children=False,
    )

    # Старая, но не терминальная
    await create_task_run_with_children(
        sa_task_run_repo, sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
        task_id=tasks[1].id, group_name="g4",
        status=TaskRunStatus.EXECUTION, status_updated_at=now - timedelta(days=100),
        with_children=False,
    )

    rs = await cleanup_task_runs_uc.apply(CleanupTaskRunsUCRq())

    assert rs.success is True
    assert rs.deleted_count == 0

    remaining = await sa_task_run_repo.get_all()
    assert len(remaining) == 2


async def test_all_terminal_statuses_are_eligible(
        cleanup_task_runs_uc,
        sa_task_repo,
        sa_task_group_repo,
        sa_monitoring_algorithm_repo,
        sa_payload_repo,
        sa_task_run_repo,
        sa_task_run_status_log_repo,
        sa_task_run_time_interval_execution_bounds_repo,
        sa_task_run_time_interval_progress_repo,
):
    statuses = [TaskRunStatus.SUCCEED, TaskRunStatus.CANCELLED, TaskRunStatus.ERROR]

    tasks = await create_tasks(
        sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
        sa_payload_repo, group_name="g5", tasks_amount=len(statuses), task_status=TaskStatus.SUCCEED,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    for task, status in zip(tasks, statuses):
        await create_task_run_with_children(
            sa_task_run_repo, sa_task_run_status_log_repo,
            sa_task_run_time_interval_execution_bounds_repo, sa_task_run_time_interval_progress_repo,
            task_id=task.id, group_name="g5",
            status=status, status_updated_at=now - timedelta(days=40),
            with_children=False,
        )

    rs = await cleanup_task_runs_uc.apply(CleanupTaskRunsUCRq())

    assert rs.deleted_count == len(statuses)
    remaining = await sa_task_run_repo.get_all()
    assert len(remaining) == 0

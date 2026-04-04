import pytest

from service.domain.schemas.enums import TaskRunStatus
from tests.utils import create_tasks, create_tasks_runs


@pytest.mark.asyncio
async def test_provide(sa_waiting_task_run_provider, sa_task_repo, sa_payload_repo,
                       sa_task_group_repo, sa_monitoring_algorithm_repo, sa_task_run_repo, ):
    group_name = 'test'
    tasks_amount = 10
    tasks =await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                              sa_payload_repo, group_name, tasks_amount)
    task_runs = await create_tasks_runs(sa_task_run_repo, tasks, group_name, TaskRunStatus.WAITING)

    waiting_task_runs = await sa_waiting_task_run_provider.provide({group_name: tasks_amount})
    assert len(waiting_task_runs) == tasks_amount

    tasks_amount_decreased = tasks_amount - 5
    waiting_task_runs = await sa_waiting_task_run_provider.provide({group_name: tasks_amount_decreased})
    assert len(waiting_task_runs) == tasks_amount_decreased

    tasks_amount_increased = tasks_amount + 5
    waiting_task_runs = await sa_waiting_task_run_provider.provide({group_name: tasks_amount_increased})
    assert len(waiting_task_runs) == tasks_amount

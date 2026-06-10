import pytest

from imitation_modelling.schemas import TaskRunStatus
from service.domain.schemas.enums import TaskStatus
from tests.utils import create_tasks, create_tasks_runs


@pytest.mark.asyncio
async def test_one_run_providing(recent_task_runs_provider, sa_task_repo,
                                 sa_task_group_repo,
                                 sa_task_run_repo, sa_monitoring_algorithm_repo, sa_payload_repo,):
    tasks_amount = 3
    group_name = "test_group"
    tasks = await create_tasks(sa_task_repo, sa_task_group_repo, sa_monitoring_algorithm_repo,
                               sa_payload_repo, group_name, tasks_amount, TaskStatus.SUCCEED)
    tasks_runs = await create_tasks_runs(sa_task_run_repo, tasks, group_name, TaskRunStatus.SUCCEED)
    assert len(tasks_runs) == tasks_amount
    recent_per_task = await recent_task_runs_provider.get_recent_per_task([task.id for task in tasks], tasks_amount)
    assert len(recent_per_task) == len(tasks_runs)

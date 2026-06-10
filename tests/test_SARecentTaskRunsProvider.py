import pytest


@pytest.mark.asyncio
async def test_one_run_providing(recent_task_runs_provider):
    recent_per_task = await recent_task_runs_provider.get_recent_per_task([857, 427], 3)
    assert len(recent_per_task) == 6

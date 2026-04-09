import pprint

import pytest

from service.domain.use_cases.external.get_task_group_statistics import GetAllTaskGroupStatisticsUCRq


@pytest.mark.asyncio
async def test_apply(get_all_task_group_statistics_uc):
    rs = await get_all_task_group_statistics_uc.apply(GetAllTaskGroupStatisticsUCRq())
    assert False, (rs.model_dump())
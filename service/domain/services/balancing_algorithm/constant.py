from typing import Dict

from service.domain.schemas.task_group import TaskGroupPK, TaskGroup
from service.domain.schemas.task_run import TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.ports.outbound.repo.abstract import Repo


class ConstantBalancingAlgorithm(BalancingAlgorithm):
    async def calculate_batch_size_by_group(self) -> Dict[str, int]:
        task_groups = await self._task_group_repo.get_all()
        return {task_group.name: self._batch_size for task_group in task_groups}

    def __init__(self,
                 batch_size: int,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK], ):
        super().__init__( task_group_repo)
        self._batch_size = batch_size

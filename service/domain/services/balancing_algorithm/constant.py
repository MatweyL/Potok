from typing import Dict, List

from service.domain.schemas.task_group import TaskGroupPK, TaskGroup
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.ports.outbound.repo.abstract import Repo


class ConstantBalancingAlgorithm(BalancingAlgorithm):
    def __init__(self,
                 batch_size: int,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK], ):
        super().__init__(task_group_repo)
        self._batch_size = batch_size

    async def calculate_batch_size_by_group(self, group_names: List[str]) -> Dict[str, int]:
        return {group_name: self._batch_size for group_name in group_names}

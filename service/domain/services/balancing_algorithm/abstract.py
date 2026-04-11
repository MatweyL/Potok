from abc import ABC, abstractmethod
from typing import Dict, List

from service.domain.schemas.task_group import TaskGroupPK, TaskGroup
from service.ports.outbound.repo.abstract import Repo


class BalancingAlgorithm(ABC):

    def __init__(self, task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK], ):
        self._task_group_repo = task_group_repo

    @abstractmethod
    async def calculate_batch_size_by_group(self, group_names: List[str]) -> Dict[str, int]:
        pass

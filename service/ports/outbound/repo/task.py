from abc import ABC, abstractmethod
from typing import List, Dict

from pydantic import BaseModel


class TasksToTransitStatus(BaseModel):
    succeed_ids: List[int]
    error_ids: List[int]


class TaskProvider(ABC):
    @abstractmethod
    async def provide_tasks_ids_to_transit_via_sql(self, ) -> TasksToTransitStatus:
        pass


class TaskGroupsStatistics(BaseModel):
    total_tasks_count: int
    tasks_count_by_group_name: Dict[str, int]


class TaskStatisticsProvider(ABC):

    @abstractmethod
    async def provide_groups_statistics(self) -> TaskGroupsStatistics:
        pass

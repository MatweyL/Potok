from abc import ABC, abstractmethod
from typing import List

from pydantic import BaseModel


class TasksToTransitStatus(BaseModel):
    succeed_ids: List[int]
    error_ids: List[int]


class TaskProvider(ABC):
    @abstractmethod
    async def provide_tasks_ids_to_transit_via_sql(self, ) -> TasksToTransitStatus:
        pass

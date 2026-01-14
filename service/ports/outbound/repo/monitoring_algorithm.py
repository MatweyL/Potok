import asyncio
from abc import ABC, abstractmethod
from typing import List

from service.domain.schemas.task import Task
from service.ports.outbound.repo.abstract import Repo


class TaskToExecuteProvider(ABC):
    @abstractmethod
    async def provide_tasks_to_execute(self) -> List[Task]:
        pass


class MonitoringAlgorithmRepo(Repo, TaskToExecuteProvider, ABC):
    pass


class TaskToExecuteProviderRegistry(TaskToExecuteProvider):
    def __init__(self, monitoring_algorithm_repos: List[MonitoringAlgorithmRepo]):
        self._monitoring_algorithm_repos = monitoring_algorithm_repos

    async def provide_tasks_to_execute(self) -> List[Task]:
        tasks_lists = await asyncio.gather(*[monitoring_algorithm_repo.provide_tasks_to_execute()
                                             for monitoring_algorithm_repo in self._monitoring_algorithm_repos])
        tasks_to_execute = []
        for tasks in tasks_lists:
            tasks_to_execute.extend(tasks)
        return tasks_to_execute

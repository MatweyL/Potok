from abc import ABC, abstractmethod

from service.domain.schemas.enums import PriorityType, TaskType
from service.domain.schemas.task_run import TaskRunStatusLogPK, TaskRunStatusLog
from service.ports.outbound.repo.abstract import Repo


class BalancingAlgorithm(ABC):

    def __init__(self, task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK]):
        self._task_run_status_log_repo = task_run_status_log_repo

    @abstractmethod
    async def calculate_batch_size(self, group_name: str, priority_type: PriorityType, task_type: TaskType) -> int:
        pass

from abc import ABC, abstractmethod
from typing import Dict, List

from service.domain.schemas.task_run import TaskRun
from service.domain.schemas.task_run_metrics import TaskRunMetrics


class WaitingTaskRunProvider(ABC):
    @abstractmethod
    async def provide(self, amount_by_group_name: Dict[str, int]) -> List[TaskRun]:
        pass


class TaskRunMetricsProvider(ABC):
    @abstractmethod
    async def provide_by_period(self, period_s: int) -> TaskRunMetrics:
        pass

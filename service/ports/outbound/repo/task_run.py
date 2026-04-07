from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from service.domain.schemas.task_run import TaskRun
from service.domain.schemas.task_run_metrics import TaskRunMetrics, TaskRunAvgMetrics


class WaitingTaskRunProvider(ABC):
    @abstractmethod
    async def provide(self, amount_by_group_name: Dict[str, int]) -> List[TaskRun]:
        pass


class TaskRunMetricsProvider(ABC):
    @abstractmethod
    async def provide_by_period(self, period_s: int,
                                group_name: Union[Optional[str], List[str]] = None) -> TaskRunMetrics:
        pass

    @abstractmethod
    async def provide_avg_by_period(self, period_s: int,
                                    group_name: Union[Optional[str], List[str]] = None) -> TaskRunAvgMetrics:
        pass

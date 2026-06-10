from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from service.domain.schemas.task_run import TaskRun, TaskRunTimeIntervalExecutionBounds
from service.domain.schemas.task_run_metrics import TaskRunMetrics, TaskRunAvgMetrics, StatusMetrics, \
    TasksRunsStatusMetrics


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

    @abstractmethod
    async def provide_tasks_runs_status_metrics(self, tasks_ids: List[int]) -> TasksRunsStatusMetrics:
        pass


class RecentTaskRunsProvider(ABC):

    @abstractmethod
    async def get_recent_per_task(
            self,
            task_ids: List[int],
            limit_per_task: int,
    ) -> List[TaskRun]:
        pass


class LatestTaskRunTimeIntervalExecutionBoundsProvider(ABC):
    @abstractmethod
    async def provide_latest_bounds_by_task_ids(
            self, task_ids: List[int]
    ) -> Dict[int, TaskRunTimeIntervalExecutionBounds]:
        pass

from abc import ABC, abstractmethod
from typing import List, Optional

from service.domain.schemas.analytical_metrics import (
    DashboardSummaryMetrics,
    DurationDistributionItem,
    PerformanceTrendItem,
    RunHeatmapItem,
    RunStatusDistributionItem,
    TaskGroupProcessingSpeedItem,
    TaskRunStatistics,
)


class AnalyticalMetricsProviderI(ABC):
    @abstractmethod
    async def get_dashboard_summary(self, day_offset: int = 0) -> DashboardSummaryMetrics:
        pass

    @abstractmethod
    async def get_run_status_distribution(self, group_id: Optional[int] = None) -> List[RunStatusDistributionItem]:
        pass

    @abstractmethod
    async def get_performance_trends(self, period: str) -> List[PerformanceTrendItem]:
        pass

    @abstractmethod
    async def get_run_heatmap(self) -> List[RunHeatmapItem]:
        pass

    @abstractmethod
    async def get_duration_distribution(self, group_id: Optional[int] = None) -> List[DurationDistributionItem]:
        pass

    @abstractmethod
    async def get_task_group_processing_speed(self, group_id: int) -> List[TaskGroupProcessingSpeedItem]:
        pass

    @abstractmethod
    async def get_task_run_statistics(self, task_id: int) -> TaskRunStatistics:
        pass

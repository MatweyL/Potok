from typing import List, Optional

from service.domain.schemas.analytical_metrics import (
    DashboardSummaryMetrics,
    DashboardSummaryWithDelta,
    DurationDistributionItem,
    PerformanceTrendItem,
    RunHeatmapItem,
    RunStatusDistributionItem,
    TaskGroupProcessingSpeedItem,
    TaskRunStatistics,
)
from service.ports.outbound.repo.analytical_metrics import AnalyticalMetricsProviderI


class AnalyticalMetricsService:
    def __init__(self, analytical_metrics_provider: AnalyticalMetricsProviderI):
        self._analytical_metrics_provider = analytical_metrics_provider

    async def get_dashboard_summary(self) -> DashboardSummaryWithDelta:
        current = await self._analytical_metrics_provider.get_dashboard_summary(day_offset=0)
        previous = await self._analytical_metrics_provider.get_dashboard_summary(day_offset=-1)
        return DashboardSummaryWithDelta(
            **current.model_dump(),
            **self._calculate_delta_fields(current, previous),
        )

    async def get_run_status_distribution(self, group_id: Optional[int] = None) -> List[RunStatusDistributionItem]:
        return await self._analytical_metrics_provider.get_run_status_distribution(group_id=group_id)

    async def get_performance_trends(self, period: str) -> List[PerformanceTrendItem]:
        return await self._analytical_metrics_provider.get_performance_trends(period=period)

    async def get_run_heatmap(self) -> List[RunHeatmapItem]:
        return await self._analytical_metrics_provider.get_run_heatmap()

    async def get_duration_distribution(self, group_id: Optional[int] = None) -> List[DurationDistributionItem]:
        return await self._analytical_metrics_provider.get_duration_distribution(group_id=group_id)

    async def get_task_group_processing_speed(self, group_id: int) -> List[TaskGroupProcessingSpeedItem]:
        return await self._analytical_metrics_provider.get_task_group_processing_speed(group_id=group_id)

    async def get_task_run_statistics(self, task_id: int) -> TaskRunStatistics:
        return await self._analytical_metrics_provider.get_task_run_statistics(task_id=task_id)

    @staticmethod
    def _calculate_delta_fields(
        current: DashboardSummaryMetrics,
        previous: DashboardSummaryMetrics,
    ) -> dict[str, Optional[float]]:
        current_values = current.model_dump()
        previous_values = previous.model_dump()
        return {
            f"{metric}_delta_percent": AnalyticalMetricsService._calculate_delta(
                current_values.get(metric),
                previous_values.get(metric),
            )
            for metric in (
                "active_runs",
                "completed_today",
                "errors_today",
                "avg_duration_seconds",
            )
        }

    @staticmethod
    def _calculate_delta(current_value: int | float | None, previous_value: int | float | None) -> Optional[float]:
        current_number = current_value or 0
        previous_number = previous_value or 0
        if previous_number == 0:
            return None if current_number == 0 else 100.0
        return round((current_number - previous_number) * 100.0 / previous_number, 1)

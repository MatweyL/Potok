from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel


class DashboardSummaryMetrics(BaseModel):
    active_runs: int = 0
    completed_today: int = 0
    errors_today: int = 0
    avg_duration_seconds: Optional[int] = None


class DashboardSummaryWithDelta(DashboardSummaryMetrics):
    active_runs_delta_percent: Optional[float] = None
    completed_today_delta_percent: Optional[float] = None
    errors_today_delta_percent: Optional[float] = None
    avg_duration_seconds_delta_percent: Optional[float] = None


class RunStatusDistributionItem(BaseModel):
    status: str
    run_count: int
    percentage: Optional[float] = None


class PerformanceTrendItem(BaseModel):
    period: datetime
    completed_count: int
    avg_duration_seconds: Optional[int] = None


class RunHeatmapItem(BaseModel):
    day_of_week: int
    hour_of_day: int
    run_count: int


class DurationDistributionItem(BaseModel):
    duration_bucket: str
    run_count: int


class TaskGroupProcessingSpeedItem(BaseModel):
    period: datetime
    tasks_per_second: float
    avg_tasks_per_second: float


class TaskRunStatistics(BaseModel):
    success_count: int = 0
    error_count: int = 0
    success_rate_percent: Optional[float] = None

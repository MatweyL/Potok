from typing import Any, Optional, TypeVar

from pydantic import BaseModel
from sqlalchemy import text

from service.adapters.outbound.repo.sa.database import Database
from service.domain.schemas.analytical_metrics import (
    DashboardSummaryMetrics,
    DurationDistributionItem,
    PerformanceTrendItem,
    RunHeatmapItem,
    RunStatusDistributionItem,
    TaskGroupProcessingSpeedItem,
    TaskRunStatistics,
)
from service.ports.outbound.repo.analytical_metrics import AnalyticalMetricsProviderI

TMetric = TypeVar("TMetric", bound=BaseModel)


class SAAnalyticalMetricsProvider(AnalyticalMetricsProviderI):
    def __init__(self, database: Database):
        self._database = database

    async def get_dashboard_summary(self, day_offset: int = 0) -> DashboardSummaryMetrics:
        row = await self._fetch_one("""
            SELECT
                COUNT(*) FILTER (WHERE status IN ('EXECUTION', 'QUEUED')) AS active_runs,
                COUNT(*) FILTER (
                    WHERE status = 'SUCCEED'
                      AND status_updated_at >= CURRENT_DATE + make_interval(days => :day_offset)
                      AND status_updated_at < CURRENT_DATE + make_interval(days => :next_day_offset)
                ) AS completed_today,
                COUNT(*) FILTER (
                    WHERE status IN ('TEMP_ERROR', 'INTERRUPTED')
                      AND status_updated_at >= CURRENT_DATE + make_interval(days => :day_offset)
                      AND status_updated_at < CURRENT_DATE + make_interval(days => :next_day_offset)
                ) AS errors_today,
                ROUND(AVG(EXTRACT(EPOCH FROM (status_updated_at - loaded_at))) FILTER (
                    WHERE status = 'SUCCEED'
                      AND status_updated_at >= CURRENT_DATE + make_interval(days => :day_offset)
                      AND status_updated_at < CURRENT_DATE + make_interval(days => :next_day_offset)
                )) AS avg_duration_seconds
            FROM task_run
        """, {"day_offset": day_offset, "next_day_offset": day_offset + 1})
        return DashboardSummaryMetrics.model_validate(row)

    async def get_run_status_distribution(self, group_id: Optional[int] = None) -> list[RunStatusDistributionItem]:
        join_sql, params = self._group_filter_sql(group_id)
        rows = await self._fetch_all(f"""
            SELECT tr.status, COUNT(*) AS run_count,
                   ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) AS percentage
            FROM task_run tr
            {join_sql}
            WHERE tr.loaded_at >= NOW() - INTERVAL '24 hours'
            GROUP BY tr.status
            ORDER BY run_count DESC
        """, params)
        return self._validate_many(RunStatusDistributionItem, rows)

    async def get_performance_trends(self, period: str) -> list[PerformanceTrendItem]:
        trunc = "hour" if period == "day" else "day"
        interval = "24 hours" if period == "day" else "7 days"
        rows = await self._fetch_all(f"""
            SELECT DATE_TRUNC('{trunc}', status_updated_at) AS period,
                   COUNT(*) FILTER (WHERE status = 'SUCCEED') AS completed_count,
                   ROUND(AVG(EXTRACT(EPOCH FROM (status_updated_at - loaded_at))) FILTER (WHERE status = 'SUCCEED')) AS avg_duration_seconds
            FROM task_run
            WHERE status_updated_at >= NOW() - INTERVAL '{interval}'
            GROUP BY DATE_TRUNC('{trunc}', status_updated_at)
            ORDER BY period
        """)
        return self._validate_many(PerformanceTrendItem, rows)

    async def get_run_heatmap(self) -> list[RunHeatmapItem]:
        rows = await self._fetch_all("""
            SELECT EXTRACT(ISODOW FROM status_updated_at)::int AS day_of_week,
                   EXTRACT(HOUR FROM status_updated_at)::int AS hour_of_day,
                   COUNT(*) AS run_count
            FROM task_run
            WHERE status_updated_at >= NOW() - INTERVAL '7 days'
            GROUP BY EXTRACT(ISODOW FROM status_updated_at), EXTRACT(HOUR FROM status_updated_at)
            ORDER BY day_of_week, hour_of_day
        """)
        return self._validate_many(RunHeatmapItem, rows)

    async def get_duration_distribution(self, group_id: Optional[int] = None) -> list[DurationDistributionItem]:
        join_sql, params = self._group_filter_sql(group_id)
        rows = await self._fetch_all(f"""
            SELECT CASE
                WHEN duration_sec < 30 THEN '0-30s'
                WHEN duration_sec < 60 THEN '30-60s'
                WHEN duration_sec < 90 THEN '60-90s'
                WHEN duration_sec < 120 THEN '90-120s'
                WHEN duration_sec < 150 THEN '120-150s'
                WHEN duration_sec < 180 THEN '150-180s'
                WHEN duration_sec < 210 THEN '180-210s'
                ELSE '210+s'
            END AS duration_bucket,
            COUNT(*) AS run_count
            FROM (
                SELECT EXTRACT(EPOCH FROM (tr.status_updated_at - tr.loaded_at)) AS duration_sec
                FROM task_run tr
                {join_sql}
                WHERE tr.status = 'SUCCEED' AND tr.loaded_at >= NOW() - INTERVAL '24 hours'
            ) sub
            GROUP BY duration_bucket
            ORDER BY MIN(duration_sec)
        """, params)
        return self._validate_many(DurationDistributionItem, rows)

    async def get_task_group_processing_speed(self, group_id: int) -> list[TaskGroupProcessingSpeedItem]:
        rows = await self._fetch_all("""
            WITH bounds AS (
                SELECT DATE_TRUNC('minute', NOW() - INTERVAL '3 hours') AS date_from,
                       DATE_TRUNC('minute', NOW()) AS date_to
            ),
            runs_per_minute AS (
                SELECT DATE_TRUNC('minute', tr.status_updated_at) AS period,
                       COUNT(*) AS completed_count
                FROM task_run tr
                JOIN task t ON tr.task_id = t.id
                CROSS JOIN bounds b
                WHERE t.group_id = :group_id
                  AND tr.status = 'SUCCEED'
                  AND tr.status_updated_at >= b.date_from
                  AND tr.status_updated_at <= b.date_to
                GROUP BY DATE_TRUNC('minute', tr.status_updated_at)
            ),
            timeline AS (
                SELECT generate_series(b.date_from, b.date_to, INTERVAL '1 minute') AS period
                FROM bounds b
            )
            SELECT timeline.period AS period,
                   ROUND(COALESCE(runs_per_minute.completed_count, 0) / 60.0, 2) AS tasks_per_second,
                   ROUND(AVG(COALESCE(runs_per_minute.completed_count, 0) / 60.0) OVER (ORDER BY timeline.period ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 2) AS avg_tasks_per_second
            FROM timeline
            LEFT JOIN runs_per_minute ON timeline.period = runs_per_minute.period
            ORDER BY timeline.period
        """, {"group_id": group_id})
        return self._validate_many(TaskGroupProcessingSpeedItem, rows)

    async def get_task_run_statistics(self, task_id: int) -> TaskRunStatistics:
        row = await self._fetch_one("""
            SELECT COUNT(*) FILTER (WHERE status = 'SUCCEED') AS success_count,
                   COUNT(*) FILTER (WHERE status IN ('TEMP_ERROR', 'INTERRUPTED')) AS error_count,
                   ROUND(COUNT(*) FILTER (WHERE status = 'SUCCEED') * 100.0 / NULLIF(COUNT(*), 0), 1) AS success_rate_percent
            FROM task_run
            WHERE task_id = :task_id
        """, {"task_id": task_id})
        return TaskRunStatistics.model_validate(row)

    async def _fetch_all(self, sql: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        async with self._database.session as session:
            result = await session.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result.fetchall()]

    async def _fetch_one(self, sql: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        rows = await self._fetch_all(sql, params)
        return rows[0] if rows else {}

    @staticmethod
    def _group_filter_sql(group_id: Optional[int]) -> tuple[str, dict[str, Any]]:
        if group_id is None:
            return "", {}
        return "JOIN task t ON tr.task_id = t.id AND t.group_id = :group_id", {"group_id": group_id}

    @staticmethod
    def _validate_many(model: type[TMetric], rows: list[dict[str, Any]]) -> list[TMetric]:
        return [model.model_validate(row) for row in rows]

from typing import Any, Optional, TypeVar

from clickhouse_connect.driver.asyncclient import AsyncClient
from pydantic import BaseModel

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


class CHAnalyticalMetricsProvider(AnalyticalMetricsProviderI):
    def __init__(self, client: AsyncClient):
        self._client = client

    async def get_dashboard_summary(self, day_offset: int = 0) -> DashboardSummaryMetrics:
        row = await self._fetch_one("""
            SELECT
                countIf(status IN ('EXECUTION', 'QUEUED')) AS active_runs,
                countIf(
                    status = 'SUCCEED'
                    AND status_updated_at >= toStartOfDay(now()) + toIntervalDay({day_offset:Int32})
                    AND status_updated_at < toStartOfDay(now()) + toIntervalDay({next_day_offset:Int32})
                ) AS completed_today,
                countIf(
                    status IN ('TEMP_ERROR', 'INTERRUPTED')
                    AND status_updated_at >= toStartOfDay(now()) + toIntervalDay({day_offset:Int32})
                    AND status_updated_at < toStartOfDay(now()) + toIntervalDay({next_day_offset:Int32})
                ) AS errors_today,
                if(
                    countIf(
                        status = 'SUCCEED'
                        AND status_updated_at >= toStartOfDay(now()) + toIntervalDay({day_offset:Int32})
                        AND status_updated_at < toStartOfDay(now()) + toIntervalDay({next_day_offset:Int32})
                    ) = 0,
                    NULL,
                    round(avgIf(
                        dateDiff('second', loaded_at, status_updated_at),
                        status = 'SUCCEED'
                        AND status_updated_at >= toStartOfDay(now()) + toIntervalDay({day_offset:Int32})
                        AND status_updated_at < toStartOfDay(now()) + toIntervalDay({next_day_offset:Int32})
                    ))
                ) AS avg_duration_seconds
            FROM task_run
        """, {"day_offset": day_offset, "next_day_offset": day_offset + 1})
        return DashboardSummaryMetrics.model_validate(row)

    async def get_run_status_distribution(self, group_id: Optional[int] = None) -> list[RunStatusDistributionItem]:
        join_sql, params = self._group_filter_sql(group_id)
        rows = await self._fetch_all(f"""
            SELECT status,
                   run_count,
                   round(run_count * 100.0 / nullIf(sum(run_count) OVER (), 0), 1) AS percentage
            FROM (
                SELECT tr.status AS status, count() AS run_count
                FROM task_run tr
                {join_sql}
                WHERE tr.loaded_at >= now() - INTERVAL 24 HOUR
                GROUP BY tr.status
            )
            ORDER BY run_count DESC
        """, params)
        return self._validate_many(RunStatusDistributionItem, rows)

    async def get_performance_trends(self, period: str) -> list[PerformanceTrendItem]:
        trunc = "toStartOfHour" if period == "day" else "toStartOfDay"
        interval = "INTERVAL 24 HOUR" if period == "day" else "INTERVAL 7 DAY"
        rows = await self._fetch_all(f"""
            SELECT {trunc}(status_updated_at) AS period,
                   countIf(status = 'SUCCEED') AS completed_count,
                   if(
                       countIf(status = 'SUCCEED') = 0,
                       NULL,
                       round(avgIf(dateDiff('second', loaded_at, status_updated_at), status = 'SUCCEED'))
                   ) AS avg_duration_seconds
            FROM task_run
            WHERE status_updated_at >= now() - {interval}
            GROUP BY period
            ORDER BY period
        """)
        return self._validate_many(PerformanceTrendItem, rows)

    async def get_run_heatmap(self) -> list[RunHeatmapItem]:
        rows = await self._fetch_all("""
            SELECT toDayOfWeek(status_updated_at) AS day_of_week,
                   toHour(status_updated_at) AS hour_of_day,
                   count() AS run_count
            FROM task_run
            WHERE status_updated_at >= now() - INTERVAL 7 DAY
            GROUP BY day_of_week, hour_of_day
            ORDER BY day_of_week, hour_of_day
        """)
        return self._validate_many(RunHeatmapItem, rows)

    async def get_duration_distribution(self, group_id: Optional[int] = None) -> list[DurationDistributionItem]:
        join_sql, params = self._group_filter_sql(group_id)
        rows = await self._fetch_all(f"""
            SELECT multiIf(
                duration_sec < 30, '0-30s',
                duration_sec < 60, '30-60s',
                duration_sec < 90, '60-90s',
                duration_sec < 120, '90-120s',
                duration_sec < 150, '120-150s',
                duration_sec < 180, '150-180s',
                duration_sec < 210, '180-210s',
                '210+s'
            ) AS duration_bucket,
            count() AS run_count
            FROM (
                SELECT dateDiff('second', tr.loaded_at, tr.status_updated_at) AS duration_sec
                FROM task_run tr
                {join_sql}
                WHERE tr.status = 'SUCCEED' AND tr.loaded_at >= now() - INTERVAL 24 HOUR
            )
            GROUP BY duration_bucket
            ORDER BY min(duration_sec)
        """, params)
        return self._validate_many(DurationDistributionItem, rows)

    async def get_task_group_processing_speed(self, group_id: int) -> list[TaskGroupProcessingSpeedItem]:
        rows = await self._fetch_all("""
            WITH runs_per_minute AS (
                SELECT toStartOfMinute(tr.status_updated_at) AS period,
                       count() AS completed_count
                FROM task_run tr
                INNER JOIN task t ON tr.task_id = t.id
                WHERE t.group_id = {group_id:Int64}
                  AND tr.status = 'SUCCEED'
                  AND tr.status_updated_at >= now() - INTERVAL 3 HOUR
                GROUP BY period
            )
            SELECT period,
                   round(completed_count / 60.0, 2) AS tasks_per_second,
                   round(avg(completed_count / 60.0) OVER (ORDER BY period ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 2) AS avg_tasks_per_second
            FROM runs_per_minute
            ORDER BY period
        """, {"group_id": group_id})
        return self._validate_many(TaskGroupProcessingSpeedItem, rows)

    async def get_task_run_statistics(self, task_id: int) -> TaskRunStatistics:
        row = await self._fetch_one("""
            SELECT countIf(status = 'SUCCEED') AS success_count,
                   countIf(status IN ('TEMP_ERROR', 'INTERRUPTED')) AS error_count,
                   round(countIf(status = 'SUCCEED') * 100.0 / nullIf(count(), 0), 1) AS success_rate_percent
            FROM task_run
            WHERE task_id = {task_id:Int64}
        """, {"task_id": task_id})
        return TaskRunStatistics.model_validate(row)

    async def _fetch_all(self, sql: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        result = await self._client.query(sql, parameters=params or {})
        return [dict(row) for row in result.named_results()]

    async def _fetch_one(self, sql: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        rows = await self._fetch_all(sql, params)
        return rows[0] if rows else {}

    @staticmethod
    def _group_filter_sql(group_id: Optional[int]) -> tuple[str, dict[str, Any]]:
        if group_id is None:
            return "", {}
        return "INNER JOIN task t ON tr.task_id = t.id AND t.group_id = {group_id:Int64}", {"group_id": group_id}

    @staticmethod
    def _validate_many(model: type[TMetric], rows: list[dict[str, Any]]) -> list[TMetric]:
        return [model.model_validate(row) for row in rows]

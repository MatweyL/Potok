from datetime import datetime, timedelta
from typing import Dict, List

from sqlalchemy import text, select, union_all

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.database import Database
from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.execution_bounds import as_execution_bounds
from service.domain.schemas.payload import Payload
from service.domain.schemas.task_run import TaskRun, TaskRunPK
from service.domain.schemas.task_run_metrics import TaskRunMetrics, TaskRunGroupedMetrics
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.task_run import WaitingTaskRunProvider, TaskRunMetricsProvider


class SATaskRunRepo(AbstractSARepo):
    def to_model(self, obj: TaskRun) -> models.TaskRun:
        payload = obj.payload.model_dump() if obj.payload else None
        execution_bounds = obj.execution_bounds.model_dump() if obj.execution_bounds else None
        return models.TaskRun(id=obj.id,
                              task_id=obj.task_id,
                              group_name=obj.group_name,
                              priority=obj.priority,
                              type=obj.type,
                              payload=payload,
                              execution_bounds=execution_bounds,
                              execution_arguments=obj.execution_arguments,
                              status=obj.status,
                              status_updated_at=obj.status_updated_at,
                              description=obj.description)

    def to_domain(self, obj: models.TaskRun) -> TaskRun:
        payload = Payload.model_validate(obj.payload, from_attributes=True) if obj.payload else None
        execution_bounds = None
        if obj.execution_bounds:
            execution_bounds = as_execution_bounds(obj.execution_bounds)
        return TaskRun(id=obj.id,
                       task_id=obj.task_id,
                       group_name=obj.group_name,
                       priority=obj.priority,
                       type=obj.type,
                       payload=payload,
                       execution_bounds=execution_bounds,
                       execution_arguments=obj.execution_arguments,
                       status=obj.status,
                       status_updated_at=obj.status_updated_at,
                       description=obj.description,
                       )

    def pk_to_model_pk(self, pk: TaskRunPK) -> Dict:
        return {"id": pk.id}


class SAWaitingTaskRunProvider(WaitingTaskRunProvider):

    def __init__(self, database: Database, task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK], ):
        self._database = database
        self._task_run_repo = task_run_repo

    async def provide(self, amount_by_group_name: Dict[str, int]) -> List[TaskRun]:
        if not amount_by_group_name:
            return []
            # Строим подзапросы для каждой группы
        subqueries = []

        for group_name, amount in amount_by_group_name.items():
            subquery = (
                select(models.TaskRun)  # используем вашу ORM-модель
                .where(models.TaskRun.group_name == group_name)
                .where(models.TaskRun.status == 'WAITING')
                .order_by(models.TaskRun.status_updated_at)
                .limit(amount)
            )
            subqueries.append(subquery)

        # Объединяем все подзапросы
        query = union_all(*subqueries)

        async with self._database.session as session:
            result = await session.execute(query)
            rows = result.mappings().fetchall()
            return [self._task_run_repo.to_domain(row) for row in rows]


class SATaskRunMetricsProvider(TaskRunMetricsProvider):
    def __init__(self, database: Database, ):
        self._database = database

    async def provide_by_period(self, period_s: int) -> TaskRunMetrics:

        bound_datetime = datetime.now() - timedelta(seconds=period_s)

        async with self._database.session as session:
            query = text("""
                SELECT group_name, status, COUNT(*) as cnt 
                FROM task_run 
                WHERE status_updated_at < :bound_datetime 
                GROUP BY group_name, status
            """)
            result = await session.execute(query, {'bound_datetime': bound_datetime})
            rows = result.fetchall()

            grouped_metrics_by_name: Dict[str, TaskRunGroupedMetrics] = {}

            for row in rows:
                group_name = row[0]
                status = row[1]
                cnt = row[2]

                # Инициализируем метрики для группы, если ещё нет
                if group_name not in grouped_metrics_by_name:
                    grouped_metrics_by_name[group_name] = TaskRunGroupedMetrics(
                        group_name=group_name,
                        period_s=period_s
                    )

                task_run_grouped_metrics = grouped_metrics_by_name[group_name]

                # Обрабатываем все возможные статусы
                if status == TaskRunStatus.SUCCEED:
                    task_run_grouped_metrics.succeed = cnt
                elif status == TaskRunStatus.WAITING:
                    task_run_grouped_metrics.waiting = cnt
                elif status == TaskRunStatus.INTERRUPTED:
                    task_run_grouped_metrics.interrupted = cnt
                elif status == TaskRunStatus.CANCELLED:
                    task_run_grouped_metrics.cancelled = cnt
                elif status == TaskRunStatus.EXECUTION:
                    task_run_grouped_metrics.execution = cnt
                elif status == TaskRunStatus.ERROR:
                    task_run_grouped_metrics.error = cnt
                elif status == TaskRunStatus.TEMP_ERROR:
                    task_run_grouped_metrics.temp_error = cnt
                elif status == TaskRunStatus.QUEUED:
                    task_run_grouped_metrics.queued = cnt

            return TaskRunMetrics(grouped_metrics_by_name=grouped_metrics_by_name)

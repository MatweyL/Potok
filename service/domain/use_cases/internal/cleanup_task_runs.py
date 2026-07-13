# service/domain/use_cases/internal/cleanup_task_runs.py

import asyncio
from datetime import datetime, timedelta, timezone

from service.domain.schemas.task_run import (
    TaskRun, TaskRunPK,
)
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import (
    FilterField,
    FilterFieldsConjunct,
    FilterFieldsDNF,
    ConditionOperation,
    PaginationQuery,
)
from service.ports.outbound.repo.transaction import TransactionFactory


class CleanupTaskRunsUCRq(UCRequest):
    pass


class CleanupTaskRunsUCRs(UCResponse):
    request: CleanupTaskRunsUCRq
    deleted_count: int


# Статусы, которые считаются "завершёнными" и подлежат очистке
TERMINAL_STATUSES = ["SUCCEED", "ERROR"]


class CleanupTaskRunsUC(UseCase):
    """
    Периодически удаляет старые завершённые запуски задач (TaskRun)
    вместе со всеми связанными записями:
      - task_run_status_log
      - task_run_time_interval_execution_bounds
      - task_run_time_interval_progress

    Удаление идёт батчами, чтобы не держать долгие транзакции
    и не блокировать БД на больших объёмах.
    """

    def __init__(
        self,
        task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
        task_run_status_log_repo: Repo,
        task_run_execution_bounds_repo: Repo,
        task_run_progress_repo: Repo,
        transaction_factory: TransactionFactory,
        retention_days: int = 30,
        batch_size: int = 10_000,
        pause_seconds: float = 0.05,
    ):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._task_run_execution_bounds_repo = task_run_execution_bounds_repo
        self._task_run_progress_repo = task_run_progress_repo
        self._transaction_factory = transaction_factory
        self._retention_days = retention_days
        self._batch_size = batch_size
        self._pause_seconds = pause_seconds

    async def apply(self, request: CleanupTaskRunsUCRq) -> CleanupTaskRunsUCRs:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        total_deleted = 0

        candidates_filter = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(group=[
                    FilterField(
                        name="status_updated_at",
                        value=cutoff,
                        operation=ConditionOperation.LTE,
                    ),
                    FilterField(
                        name="status",
                        value=TERMINAL_STATUSES,
                        operation=ConditionOperation.IN,
                    ),
                ])
            ]
        )

        pagination = PaginationQuery(
            filter_fields_dnf=candidates_filter,
            limit_per_page=self._batch_size,
            order_by="status_updated_at",
            asc_sort=True,
        )

        while True:
            candidates = await self._task_run_repo.paginated(pagination)
            if not candidates:
                break

            ids = [r.id for r in candidates]

            async with self._transaction_factory.create() as tx:
                # Порядок важен — сначала дочерние, потом родитель
                await self._task_run_status_log_repo.delete_by_condition(
                    FilterFieldsDNF.single("task_run_id", ids, ConditionOperation.IN),
                    transaction=tx,
                )
                await self._task_run_execution_bounds_repo.delete_by_condition(
                    FilterFieldsDNF.single("task_run_id", ids, ConditionOperation.IN),
                    transaction=tx,
                )
                await self._task_run_progress_repo.delete_by_condition(
                    FilterFieldsDNF.single("task_run_id", ids, ConditionOperation.IN),
                    transaction=tx,
                )
                await self._task_run_repo.delete_by_condition(
                    FilterFieldsDNF.single("id", ids, ConditionOperation.IN),
                    transaction=tx,
                )

            total_deleted += len(ids)
            logger.info(f"CleanupTaskRunsUC: удалено {total_deleted} запусков (cutoff={cutoff.isoformat()})")

            # Если вернулось меньше чем batch_size — это была последняя пачка
            if len(candidates) < self._batch_size:
                break

            # Пауза чтобы не давить на БД
            await asyncio.sleep(self._pause_seconds)

        logger.info(f"CleanupTaskRunsUC: завершено, всего удалено {total_deleted} запусков")
        return CleanupTaskRunsUCRs(
            success=True,
            request=request,
            deleted_count=total_deleted,
        )
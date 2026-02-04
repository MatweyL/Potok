from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import cached_property
from typing import Optional

from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunStatusLog
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, FilterField, ConditionOperation, UpdateFields
from service.ports.outbound.repo.transaction import TransactionFactory


class TransitTaskRunStatusUCRq(UCRequest):
    ttl_seconds: int


class TransitTaskRunStatusUCRs(UCResponse, ):
    request: TransitTaskRunStatusUCRq
    count: int = 0


class AbstractTransitTaskRunStatusUC(UseCase, ABC):
    """
    Переводит TaskRun из статуса 1 в статус 2, если задача пробыла
    в статусе 1 дольше, чем ttl_seconds.
    """

    def __init__(
        self,
        task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
        task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunPK],
        transaction_factory: TransactionFactory,
    ):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory

    @cached_property
    @abstractmethod
    def from_status(self) -> TaskRunStatus:
        pass

    @cached_property
    @abstractmethod
    def to_status(self) -> TaskRunStatus:
        pass


    async def apply(
        self, request: TransitTaskRunStatusUCRq
    ) -> TransitTaskRunStatusUCRs:
        # Если в запросе указано ttl - вычисляем граничную дату
        if request.ttl_seconds:
            # Вычисляем граничное время: задачи, обновлённые раньше этого момента, считаются просроченными
            threshold_time = datetime.now() - timedelta(seconds=request.ttl_seconds)

            # Находим все TaskRun в статусе 1, которые пробыли в нём дольше TTL
            filter_fields = FilterFieldsDNF.single_conjunct(
                [
                    FilterField(name="status", value=self.from_status, operation=ConditionOperation.EQ),
                    FilterField(
                        name="status_updated_at",
                        value=threshold_time,
                        operation=ConditionOperation.LT,
                    ),
                ]
            )
        else:
            # Находим все TaskRun в статусе 1, которые пробыли в нём дольше TTL
            filter_fields = FilterFieldsDNF.single(name="status", value=self.from_status)

        expired_task_runs = await self._task_run_repo.filter(filter_fields)

        if not expired_task_runs:
            return TransitTaskRunStatusUCRs(
                success=True,
                request=request,
                count=0,
            )

        # Обновляем статус всех задач
        now = datetime.utcnow()
        update_fields = UpdateFields.multiple(
            {
                "status": self.to_status,
                "status_updated_at": now,
            }
        )

        fields_by_pk = {TaskRunPK(id=tr.id): update_fields for tr in expired_task_runs}
        async with self._transaction_factory.create() as transaction:
            await self._task_run_repo.update_all(fields_by_pk, transaction)

            # Создаём записи в логе статусов
            status_logs = [
                TaskRunStatusLog(
                    task_run_id=tr.id,
                    status_updated_at=now,
                    status=self.to_status,
                )
                for tr in expired_task_runs
            ]

            await self._task_run_status_log_repo.create_all(status_logs, transaction)

        return TransitTaskRunStatusUCRs(
            success=True,
            request=request,
            count=len(expired_task_runs),
        )

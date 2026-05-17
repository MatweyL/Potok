from datetime import datetime

from more_itertools import batched

from service.domain.schemas.enums import TaskStatus, TaskRunStatus
from service.domain.schemas.task import Task, TaskPK, TaskStatusLogPK, TaskStatusLog
from service.domain.schemas.task_run import TaskRunPK, TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, PaginationQuery, ConditionOperation, FilterField, \
    UpdateFields
from service.ports.outbound.repo.task import TaskProvider
from service.ports.outbound.repo.transaction import TransactionFactory


class TransitTaskStatusUCRq(UCRequest):
    pass


class TransitTaskStatusUCRs(UCResponse):
    request: TransitTaskStatusUCRq
    succeed_count: int = 0
    error_count: int = 0


class TransitTaskStatusUC(UseCase):
    def __init__(self,
                 task_repo: Repo[Task, Task, TaskPK],
                 task_provider: TaskProvider,
                 task_status_log_repo: Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK,],
                 transaction_factory: TransactionFactory, ):
        self._task_repo = task_repo
        self._task_provider = task_provider
        self._task_status_log_repo = task_status_log_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: TransitTaskStatusUCRq) -> TransitTaskStatusUCRs:  # TODO: optimize by sql
        tasks_ids_to_transit = await self._task_provider.provide_tasks_ids_to_transit_via_sql()
        succeed_tasks_ids = tasks_ids_to_transit.succeed_ids
        error_tasks_ids = tasks_ids_to_transit.error_ids
        status_updated_at = datetime.now()
        # Формируем данные для пакетного обновления статусов
        tasks_to_update = {TaskPK(id=task_id): UpdateFields.multiple({'status': TaskStatus.SUCCEED,
                                                                      'status_updated_at': status_updated_at})
                           for task_id in succeed_tasks_ids}
        tasks_to_update.update({TaskPK(id=task_id): UpdateFields.multiple({'status': TaskStatus.ERROR,
                                                                           'status_updated_at': status_updated_at})
                                for task_id in error_tasks_ids})

        # Формируем логи смены статуса для пакетной вставки
        task_status_logs = [TaskStatusLog(task_id=task_id, status_updated_at=status_updated_at,
                                          status=TaskStatus.SUCCEED) for task_id in succeed_tasks_ids]
        task_status_logs.extend([TaskStatusLog(task_id=task_id, status_updated_at=status_updated_at,
                                               status=TaskStatus.ERROR) for task_id in error_tasks_ids])

        # Обновляем статусы
        await self._task_repo.update_all(tasks_to_update, )
        # Сохраняем в лог событие смены статуса
        await self._task_status_log_repo.create_all(task_status_logs, )

        return TransitTaskStatusUCRs(request=request,
                                     succeed_count=len(succeed_tasks_ids),
                                     error_count=len(error_tasks_ids),
                                     success=True)

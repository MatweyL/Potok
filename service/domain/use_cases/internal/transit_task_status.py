from datetime import datetime

from more_itertools import batched

from service.domain.schemas.enums import TaskStatus, TaskRunStatus
from service.domain.schemas.task import Task, TaskPK, TaskStatusLogPK, TaskStatusLog
from service.domain.schemas.task_run import TaskRunPK, TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, PaginationQuery, ConditionOperation, FilterField, \
    UpdateFields
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
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_status_log_repo: Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK,],
                 transaction_factory: TransactionFactory, ):
        self._task_repo = task_repo
        self._task_run_repo = task_run_repo
        self._task_status_log_repo = task_status_log_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: TransitTaskStatusUCRq) -> TransitTaskStatusUCRs:
        async with self._transaction_factory.create() as transaction:
            execution_tasks = await self._task_repo.filter(FilterFieldsDNF.single('status', TaskStatus.EXECUTION),
                                                           transaction)
            execution_tasks_ids = [execution_task.id for execution_task in execution_tasks]
            not_completed_task_ids = set()

            for execution_tasks_ids_part in batched(execution_tasks_ids, 10_000):
                filter_fields_dnf = FilterFieldsDNF.single_conjunct([
                    FilterField(name='status',
                                value=(TaskRunStatus.SUCCEED, TaskRunStatus.ERROR),
                                operation=ConditionOperation.NOT_IN),
                    FilterField(name='task_id',
                                value=execution_tasks_ids_part,
                                operation=ConditionOperation.IN)
                ]
                )
                task_runs = await self._task_run_repo.paginated(PaginationQuery(limit_per_page=1,
                                                                                filter_fields_dnf=filter_fields_dnf),
                                                                transaction)
                not_completed_task_ids.update([task_run.task_id for task_run in task_runs])
            executed_tasks_ids = set(execution_tasks_ids).difference(not_completed_task_ids)
            succeed_count_by_task_id = {}
            error_count_by_task_id = {}
            for executed_tasks_ids_part in batched(executed_tasks_ids, 10_000):
                filter_fields_dnf = FilterFieldsDNF.single(name='task_id',
                                                           value=executed_tasks_ids_part,
                                                           operation=ConditionOperation.IN)
                task_runs = await self._task_run_repo.paginated(PaginationQuery(limit_per_page=3,
                                                                                order_by='status_updated_at',
                                                                                asc_sort=False,
                                                                                filter_fields_dnf=filter_fields_dnf),
                                                                transaction)
                for task_run in task_runs:
                    if task_run.status == TaskRunStatus.SUCCEED:
                        try:
                            succeed_count_by_task_id[task_run.task_id] += 1
                        except KeyError:
                            succeed_count_by_task_id[task_run.task_id] = 1
                    elif task_run.status == TaskRunStatus.ERROR:
                        try:
                            error_count_by_task_id[task_run.task_id] += 1
                        except KeyError:
                            error_count_by_task_id[task_run.task_id] = 1

            succeed_tasks_ids = []
            error_tasks_ids = []
            # Определяем, какие задачи успешно выполнились, а какие нет
            for executed_task_id in executed_tasks_ids:
                succeed_count = succeed_count_by_task_id.get(executed_task_id)
                error_count = error_count_by_task_id.get(executed_task_id)
                if succeed_count is None and error_count is None:
                    continue
                if succeed_count and succeed_count > 0:
                    succeed_tasks_ids.append(executed_task_id)
                elif error_count:
                    error_tasks_ids.append(executed_task_id)

            status_updated_at = datetime.utcnow()
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
            await self._task_repo.update_all(tasks_to_update, transaction)
            # Сохраняем в лог событие смены статуса
            await self._task_status_log_repo.create_all(task_status_logs, transaction)

            return TransitTaskStatusUCRs(request=request,
                                         succeed_count=len(succeed_tasks_ids),
                                         error_count=len(error_tasks_ids),
                                         success=True)

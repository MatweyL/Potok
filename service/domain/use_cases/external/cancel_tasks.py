from datetime import datetime, timezone
from typing import List, Dict

from service.domain.schemas.enums import TaskStatus, TaskRunStatus
from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.use_cases.abstract import UCResponse, UCRequest, UseCase
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields, FilterFieldsDNF, FilterField, ConditionOperation
from service.ports.outbound.repo.transaction import TransactionFactory


class CancelTasksUCRq(UCRequest):
    tasks_ids: List[int]


class CancelTasksUCRs(UCResponse):
    request: CancelTasksUCRq
    cancelled_task_by_id: Dict[int, Task] | None = None


class CancelTasksUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK],
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 transaction_factory: TransactionFactory,):
        self._task_repo = task_repo
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: CancelTasksUCRq) -> CancelTasksUCRs:
        # TODO: можно оптимизировать несколько запросов с помощью SQL UPDATE ... SET ... WHERE ... RETURNING ...
        async with self._transaction_factory.create() as transaction:
            # 1. Обновлям статусы задач
            processing_tasks_condition = FilterFieldsDNF.single_conjunct(
                [
                    FilterField(name='status',
                                value=[TaskStatus.EXECUTION,
                                       TaskStatus.SUCCEED,
                                       TaskStatus.NEW],
                                operation=ConditionOperation.IN),
                    FilterField(name='id', value=request.tasks_ids, operation=ConditionOperation.IN)
                ]
            )
            status_updated_at = datetime.now(timezone.utc)
            update_fields = UpdateFields.multiple({'status': TaskStatus.CANCELLED,
                                                   'status_updated_at': status_updated_at})
            processing_tasks = await self._task_repo.filter(processing_tasks_condition)
            update_mapping = {processing_task: update_fields
                              for processing_task in processing_tasks}
            await self._task_repo.update_all(update_mapping, transaction)

            # 2. Обновляем статусы запусков задач
            processing_tasks_runs_condition = FilterFieldsDNF.single_conjunct(
                [
                    FilterField(name='status',
                                value=[TaskRunStatus.SUCCEED,
                                       TaskRunStatus.ERROR,
                                       TaskRunStatus.CANCELLED],
                                operation=ConditionOperation.NOT_IN),
                    FilterField(name='task_id', value=request.tasks_ids, operation=ConditionOperation.IN)
                ]
            )
            processing_tasks_runs = await self._task_run_repo.filter(processing_tasks_runs_condition)

            task_run_update_fields = UpdateFields.multiple({'status': TaskRunStatus.CANCELLED,
                                                   'status_updated_at': status_updated_at})
            task_run_update_mapping = {cancelled_task_run: task_run_update_fields
                                       for cancelled_task_run in processing_tasks_runs}
            await self._task_run_repo.update_all(task_run_update_mapping, transaction)

            # 3. Сохраняем в историю статусов запусков задач факт изменения статуса
            tasks_runs_status_logs = [TaskRunStatusLog(task_run_id=processing_task_run.id,
                                                       status_updated_at=status_updated_at,
                                                       status=TaskRunStatus.CANCELLED)
                                      for processing_task_run in processing_tasks_runs]
            await self._task_run_status_log_repo.create_all(tasks_runs_status_logs, transaction)

        return CancelTasksUCRs(success=True, request=request, cancelled_task_by_id={t.id: t for t in processing_tasks})

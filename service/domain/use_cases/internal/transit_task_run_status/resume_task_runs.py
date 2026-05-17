from typing import List
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import cached_property

from service.domain.schemas.enums import TaskRunStatus, TaskStatus
from service.domain.schemas.task import TaskPK, Task
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunStatusLog
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, FilterField, ConditionOperation, UpdateFields
from service.ports.outbound.repo.transaction import TransactionFactory


from service.domain.schemas.task_run import TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class ResumeTaskRunsUCRq(UCRequest):
    pass


class ResumeTaskRunsUCRs(UCResponse):
    request: ResumeTaskRunsUCRq
    task_runs: List[TaskRun] | None = None


class ResumeTaskRunsUC(UseCase):
    def __init__(
        self,
            task_repo: Repo[Task, Task, TaskPK],
        task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
        task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunPK],
        transaction_factory: TransactionFactory,
    ):
        self._task_repo = task_repo
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: ResumeTaskRunsUCRq) -> ResumeTaskRunsUCRs:
        cancelled_task_runs = await self._task_run_repo.filter(FilterFieldsDNF.single('status', TaskRunStatus.CANCELLED,))
        cancelled_tasks_ids = [tr.task_id for tr in cancelled_task_runs]
        resumed_tasks = await self._task_run_repo.filter(
            FilterFieldsDNF.single_conjunct(
                [
                FilterField.new('id', cancelled_tasks_ids, ConditionOperation.IN),
                FilterField.new('status', (TaskStatus.CANCELLED, TaskStatus.ERROR, TaskStatus.FINISHED), ConditionOperation.NOT_IN),]
            )
        )
        # Берем те запуски, для которых задача возобновлена
        tasks_runs_to_resume =[]
        resumed_tasks_ids = {task.id for task in resumed_tasks}
        for cancelled_task_run in cancelled_task_runs:
            if cancelled_task_run.task_id in resumed_tasks_ids:
                tasks_runs_to_resume.append(cancelled_task_run)

        # Обновляем статус подошедших задач
        now = datetime.now()
        update_fields = UpdateFields.multiple(
            {
                "status": TaskRunStatus.WAITING,
                "status_updated_at": now,
            }
        )
        fields_by_pk = {TaskRunPK(id=tr.id): update_fields for tr in tasks_runs_to_resume}
        async with self._transaction_factory.create() as transaction:
            await self._task_run_repo.update_all(fields_by_pk, transaction)

            # Создаём записи в логе статусов
            status_logs = [
                TaskRunStatusLog(
                    task_run_id=tr.id,
                    status_updated_at=now,
                    status=TaskRunStatus.WAITING,
                )
                for tr in tasks_runs_to_resume
            ]

            await self._task_run_status_log_repo.create_all(status_logs, transaction)
        return ResumeTaskRunsUCRs(request=request, success=True, task_runs=tasks_runs_to_resume)

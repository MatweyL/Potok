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


class CancelTaskRunsUCRq(UCRequest):
    pass


class CancelTaskRunsUCRs(UCResponse):
    request: CancelTaskRunsUCRq
    task_runs: List[TaskRun] | None = None


class CancelTaskRunsUC(UseCase):
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
    async def apply(self, request: CancelTaskRunsUCRq) -> CancelTaskRunsUCRs:
        cancelled_tasks = await self._task_repo.filter(FilterFieldsDNF.single('status', TaskStatus.CANCELLED))
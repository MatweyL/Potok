from datetime import datetime
from typing import List

from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF
from service.ports.outbound.repo.transaction import TransactionFactory


class RetrieveWaitingTaskRunsUCRq(UCRequest):
    pass


class RetrieveWaitingTaskRunsUCRs(UCResponse):
    request: RetrieveWaitingTaskRunsUCRq
    task_runs: List[TaskRun]


class RetrieveWaitingTaskRunsUC(UseCase):
    def __init__(self,
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 transaction_factory: TransactionFactory,
                 ):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: RetrieveWaitingTaskRunsUCRq) -> RetrieveWaitingTaskRunsUCRs:
        async with self._transaction_factory.create() as transaction:
            task_runs = await self._task_run_repo.filter(FilterFieldsDNF.single('status', TaskRunStatus.WAITING, ),
                                                         transaction)
            status_updated_at = datetime.now()
            task_run_status_logs = [TaskRunStatusLog(task_run_id=task_run.id,
                                                     status_updated_at=status_updated_at,
                                                     status=TaskRunStatus.QUEUED)
                                    for task_run in task_runs]
            await self._task_run_status_log_repo.create_all(task_run_status_logs, transaction)
        return RetrieveWaitingTaskRunsUCRs(request=request, task_runs=task_runs, success=True)

from datetime import datetime
from typing import List

from service.domain.schemas.enums import TaskRunStatus
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, UpdateFields, PaginationQuery
from service.ports.outbound.repo.task_run import WaitingTaskRunProvider
from service.ports.outbound.repo.transaction import TransactionFactory


class RetrieveWaitingTaskRunsUCRq(UCRequest):
    pass


class RetrieveWaitingTaskRunsUCRs(UCResponse):
    request: RetrieveWaitingTaskRunsUCRq
    task_runs: List[TaskRun]


class RetrieveWaitingTaskRunsUC(UseCase):
    def __init__(self,
                 task_group_repo: Repo[TaskGroup,TaskGroup,TaskGroupPK],
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 transaction_factory: TransactionFactory,
                 waiting_task_run_provider: WaitingTaskRunProvider,
                 balancing_algorithm: BalancingAlgorithm,
                 ):
        self._task_group_repo = task_group_repo
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory
        self._waiting_task_run_provider = waiting_task_run_provider
        self._balancing_algorithm = balancing_algorithm
        
    async def apply(self, request: RetrieveWaitingTaskRunsUCRq) -> RetrieveWaitingTaskRunsUCRs:
        async with self._transaction_factory.create() as transaction:
            active_groups = await self._task_group_repo.filter(FilterFieldsDNF.single('is_active', True))
            group_names = [active_group.name for active_group in active_groups]
            batch_size_by_group_name = await self._balancing_algorithm.calculate_batch_size_by_group(group_names)
            task_runs = await self._waiting_task_run_provider.provide(batch_size_by_group_name)
            status_updated_at = datetime.now()
            await self._task_run_repo.update_all({task_run: UpdateFields.multiple({
                'status': TaskRunStatus.QUEUED,
                'status_updated_at': status_updated_at,
            }) for task_run in task_runs})
            task_run_status_logs = [TaskRunStatusLog(task_run_id=task_run.id,
                                                     status_updated_at=status_updated_at,
                                                     status=TaskRunStatus.QUEUED)
                                    for task_run in task_runs]
            await self._task_run_status_log_repo.create_all(task_run_status_logs, transaction)
        return RetrieveWaitingTaskRunsUCRs(request=request, task_runs=task_runs, success=True)

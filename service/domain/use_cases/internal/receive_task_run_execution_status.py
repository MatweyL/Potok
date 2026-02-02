from service.domain.schemas.command import CommandResponse
from service.domain.schemas.task_progress import TimeIntervalTaskProgress, TimeIntervalTaskProgressPK

from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, UpdateFields
from service.ports.outbound.repo.transaction import TransactionFactory


class ReceiveTaskRunExecutionStatusUCRq(UCRequest):
    command_response: CommandResponse


class ReceiveTaskRunExecutionStatusUCRs(UCResponse):
    request: ReceiveTaskRunExecutionStatusUCRq


class ReceiveTaskRunExecutionStatusUC(UseCase):
    def __init__(self,
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 time_interval_task_progress_repo: Repo[TimeIntervalTaskProgress, TimeIntervalTaskProgress, TimeIntervalTaskProgressPK],
                 transaction_factory: TransactionFactory,
                 ):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._time_interval_task_progress_repo = time_interval_task_progress_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: ReceiveTaskRunExecutionStatusUCRq) -> ReceiveTaskRunExecutionStatusUCRs:
        command_response = request.command_response
        await self._task_run_repo.update(TaskRunPK(id=command_response.command.task_run.id),UpdateFields.multiple({
            "status": command_response.status,
            "status_updated_at": command_response.created_at,
        }))
        task_run_status_log = TaskRunStatusLog(task_run_id=command_response.command.task_run.id,
                                               status_updated_at=command_response.created_at,
                                               status=command_response.status,
                                               description=command_response.description,)
        await self._task_run_status_log_repo.create(task_run_status_log)
        if command_response.result:
            task_progress = TimeIntervalTaskProgress(task_id=command_response.command.task_run.task_id,
                                                     right_bound_at=command_response.result.right_bound_at,
                                                     left_bound_at=command_response.result.left_bound_at,
                                                     collected_data_amount=command_response.result.collected_data_amount,
                                                     saved_data_amount=command_response.result.saved_data_amount,
                                                     )
            await self._time_interval_task_progress_repo.create(task_progress)
        return ReceiveTaskRunExecutionStatusUCRs(success=True, request=request)

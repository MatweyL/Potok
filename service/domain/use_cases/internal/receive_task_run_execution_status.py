from typing import List

from service.domain.schemas.command import CommandResponse
from service.domain.schemas.task_progress import TimeIntervalTaskProgress, TimeIntervalTaskProgressPK

from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK, \
    TaskRunTimeIntervalProgress, TaskRunTimeIntervalProgressPK
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
                 task_run_time_interval_progress_repo: Repo[TaskRunTimeIntervalProgress,TaskRunTimeIntervalProgress,TaskRunTimeIntervalProgressPK],
                 transaction_factory: TransactionFactory,
                 instant_upload: bool = True,):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._time_interval_task_progress_repo = time_interval_task_progress_repo
        self._task_run_time_interval_progress_repo = task_run_time_interval_progress_repo
        self._transaction_factory = transaction_factory

        # FIXME: быстрое решение для предотвращения вставки малого количества статусов в БД (забирают все соединения)
        self._accumulated_command_responses: List[CommandResponse] = []
        self._instant_upload = instant_upload

    async def upload_command_responses(self):
        accumulated_command_responses= self._accumulated_command_responses
        self._accumulated_command_responses = []

        update_fields_by_task_run_pk = {}
        task_run_status_logs = []
        task_progresses = []
        task_run_progresses = []
        for command_response in accumulated_command_responses:
            update_fields_by_task_run_pk[TaskRunPK(id=command_response.command.task_run.id)] = UpdateFields.multiple({
                "status": command_response.status,
                "status_updated_at": command_response.created_at,
            })
            task_run_status_log = TaskRunStatusLog(task_run_id=command_response.command.task_run.id,
                                                   status_updated_at=command_response.created_at,
                                                   status=command_response.status,
                                                   description=command_response.description, )
            task_run_status_logs.append(task_run_status_log)
            if command_response.result:
                task_progress = TimeIntervalTaskProgress(task_id=command_response.command.task_run.task_id,
                                                         right_bound_at=command_response.result.right_bound_at,
                                                         left_bound_at=command_response.result.left_bound_at,
                                                         collected_data_amount=command_response.result.collected_data_amount,
                                                         saved_data_amount=command_response.result.saved_data_amount,
                                                         )
                task_progresses.append(task_progress)
                task_run_progress = TaskRunTimeIntervalProgress(task_run_id=command_response.command.task_run.id,
                                                         right_bound_at=command_response.result.right_bound_at,
                                                         left_bound_at=command_response.result.left_bound_at,
                                                         collected_data_amount=command_response.result.collected_data_amount,
                                                         saved_data_amount=command_response.result.saved_data_amount,
                                                         )
                task_run_progresses.append(task_run_progress)

        async with self._transaction_factory.create() as transaction:
            await self._task_run_repo.update_all(update_fields_by_task_run_pk, transaction)
            await self._task_run_status_log_repo.create_all(task_run_status_logs, transaction)
            await self._time_interval_task_progress_repo.create_all(task_progresses, transaction)
            await self._task_run_time_interval_progress_repo.create_all(task_run_progresses, transaction)

    async def apply(self, request: ReceiveTaskRunExecutionStatusUCRq) -> ReceiveTaskRunExecutionStatusUCRs:
        command_response = request.command_response
        self._accumulated_command_responses.append(command_response)
        if self._instant_upload:
            await self.upload_command_responses()
        return ReceiveTaskRunExecutionStatusUCRs(success=True, request=request)

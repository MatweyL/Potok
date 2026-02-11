from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.internal.retrieve_waiting_task_runs import RetrieveWaitingTaskRunsUC, \
    RetrieveWaitingTaskRunsUCRq
from service.domain.use_cases.internal.send_task_runs_to_execution import SendTaskRunsToExecutionUC, \
    SendTaskRunsToExecutionUCRq


class RetrieveAndSendTaskRunsUCRq(UCRequest):
    pass


class RetrieveAndSendTaskRunsUCRs(UCResponse):
    request: RetrieveAndSendTaskRunsUCRq


class RetrieveAndSendTaskRunsUC(UseCase):
    def __init__(self,
                 retrieve_waiting_task_runs: RetrieveWaitingTaskRunsUC,
                 send_task_runs_to_execution: SendTaskRunsToExecutionUC,):
        self._retrieve_waiting_task_runs = retrieve_waiting_task_runs
        self._send_task_runs_to_execution = send_task_runs_to_execution

    async def apply(self, request: RetrieveAndSendTaskRunsUCRq) -> RetrieveAndSendTaskRunsUCRs:
        retrieve_response = await self._retrieve_waiting_task_runs.apply(RetrieveWaitingTaskRunsUCRq())
        send_response = await self._send_task_runs_to_execution.apply(SendTaskRunsToExecutionUCRq(task_runs=retrieve_response.task_runs))
        return RetrieveAndSendTaskRunsUCRs(request=request, success=True)
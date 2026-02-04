from typing import List

from service.domain.schemas.task_run import TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.producer import DataProducerI


class SendTaskRunsToExecutionUCRq(UCRequest):
    task_runs: List[TaskRun]


class SendTaskRunsToExecutionUCRs(UCResponse):
    request: SendTaskRunsToExecutionUCRq


class SendTaskRunsToExecutionUC(UseCase):
    def __init__(self, task_runs_producer: DataProducerI):
        self._task_runs_producer = task_runs_producer

    async def apply(self, request: SendTaskRunsToExecutionUCRq) -> SendTaskRunsToExecutionUCRs:
        for task_run in request.task_runs:
            await self._task_runs_producer.produce(task_run, task_run.queue_name)
        return SendTaskRunsToExecutionUCRs(request=request, success=True)

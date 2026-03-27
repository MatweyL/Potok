from typing import List

from service.domain.schemas.command import Command
from service.domain.schemas.task_run import TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.producer import DataProducerI, QueueCreator


class SendTaskRunsToExecutionUCRq(UCRequest):
    task_runs: List[TaskRun]


class SendTaskRunsToExecutionUCRs(UCResponse):
    request: SendTaskRunsToExecutionUCRq


class SendTaskRunsToExecutionUC(UseCase):
    def __init__(self, task_runs_producer: DataProducerI, queue_creator: QueueCreator,
                 message_ttl: int = 300):
        self._task_runs_producer = task_runs_producer
        self._queue_creator = queue_creator
        self._message_ttl = message_ttl

    async def apply(self, request: SendTaskRunsToExecutionUCRq) -> SendTaskRunsToExecutionUCRs:
        for task_run in request.task_runs:
            is_queue_exists = await self._queue_creator.is_queue_exists(task_run.queue_name)
            if not is_queue_exists:
                await self._queue_creator.create_queue(task_run.queue_name)
            command = Command(task_run=task_run)
            await self._task_runs_producer.produce(command, task_run.queue_name,
                                                   item_params={'expiration': self._message_ttl})
        return SendTaskRunsToExecutionUCRs(request=request, success=True)

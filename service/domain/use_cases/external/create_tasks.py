from typing import List, Optional, Dict

from service.domain.schemas.enums import PriorityType, TaskType
from service.domain.schemas.payload import PayloadBody
from service.domain.schemas.task import TaskConfiguration
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.common.logs import logger
from service.ports.outbound.repo.transaction import TransactionFactory


class CreateTasksUCRq(UCRequest):
    payloads: List[PayloadBody]
    task_configuration: TaskConfiguration


class CreateTasksUCRs(UCResponse):
    request: CreateTasksUCRq


class CreateTasksUC(UseCase):
    def __init__(self, transaction_factory: TransactionFactory):
        self._transaction_factory = transaction_factory

    async def apply(self, request: CreateTasksUCRq) -> CreateTasksUCRs:
        try:
            async with self._transaction_factory.create() as transaction:
                uniqueness = 1
        except BaseException as e:
            logger.error(e)
            return CreateTasksUCRs(success=False, error=e, request=request)
        else:
            return CreateTasksUCRs(success=True, request=request)


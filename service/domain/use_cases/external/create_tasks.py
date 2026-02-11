from datetime import datetime
from itertools import chain
from typing import List

from service.domain.schemas.enums import TaskStatus
from service.domain.schemas.payload import PayloadBody, PayloadPK, Payload
from service.domain.schemas.task import TaskConfiguration, Task, TaskPK, TaskStatusLog, TaskStatusLogPK
from service.domain.services.uniqueness_payload_checker import UniquenessPayloadChecker
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.transaction import TransactionFactory


class CreateTasksUCRq(UCRequest):
    payloads: List[PayloadBody]
    task_configuration: TaskConfiguration


class CreateTasksUCRs(UCResponse):
    request: CreateTasksUCRq
    tasks: List[Task] = None


class CreateTasksUC(UseCase):
    def __init__(self,
                 transaction_factory: TransactionFactory,
                 uniqueness_payload_checker: UniquenessPayloadChecker,
                 payload_repo: Repo[Payload, Payload, PayloadPK],
                 task_repo: Repo[Task, Task, TaskPK],
                 task_status_log_repo: Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK]
                 ):
        self._transaction_factory = transaction_factory
        self._uniqueness_payload_checker = uniqueness_payload_checker
        self._payload_repo = payload_repo
        self._task_repo = task_repo
        self._task_status_log_repo = task_status_log_repo

    async def apply(self, request: CreateTasksUCRq) -> CreateTasksUCRs:
        try:
            async with self._transaction_factory.create() as transaction:
                checked_payload_response = await self._uniqueness_payload_checker.check(request.payloads)
                uniqueness_payloads = checked_payload_response.uniqueness
                created_payloads = await self._payload_repo.create_all(
                    [Payload.model_validate(payload_body, from_attributes=True) for payload_body in
                     uniqueness_payloads],
                    transaction
                )
                task_configuration_dict = request.task_configuration.model_dump()
                status_updated_at = datetime.utcnow()
                tasks = [Task(payload_id=payload.id,
                              status=TaskStatus.NEW,
                              status_updated_at=status_updated_at,
                              **task_configuration_dict)
                         for payload in chain(checked_payload_response.exists, created_payloads)]
                created_tasks = await self._task_repo.create_all(tasks, transaction)
                await self._task_status_log_repo.create_all([TaskStatusLog(task_id=task.id,
                                                                           status_updated_at=task.status_updated_at,
                                                                           status=task.status)
                                                             for task in created_tasks], transaction)
        except BaseException as e:
            logger.error(e)
            return CreateTasksUCRs(success=False, error=e, request=request)
        else:
            return CreateTasksUCRs(success=True, request=request, tasks=created_tasks)

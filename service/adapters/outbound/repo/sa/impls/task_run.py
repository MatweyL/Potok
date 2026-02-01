from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.execution_bounds import as_execution_bounds
from service.domain.schemas.payload import Payload
from service.domain.schemas.task_run import TaskRun, TaskRunPK


class SATaskRunRepo(AbstractSARepo):
    def to_model(self, obj: TaskRun) -> models.TaskRun:
        payload = obj.payload.model_dump() if obj.payload else None
        execution_bounds = [eb.model_dump() for eb in obj.execution_bounds] if obj.execution_bounds else None
        return models.TaskRun(id=obj.id,
                              task_id=obj.task_id,
                              group_name=obj.group_name,
                              priority=obj.priority,
                              type=obj.type,
                              payload=payload,
                              execution_bounds=execution_bounds,
                              execution_arguments=obj.execution_arguments,
                              status=obj.status,
                              status_updated_at=obj.status_updated_at,
                              description=obj.description)

    def to_domain(self, obj: models.TaskRun) -> TaskRun:
        payload = Payload.model_validate(obj.payload, from_attributes=True) if obj.payload else None
        execution_bounds = None
        if obj.execution_bounds:
            execution_bounds = [as_execution_bounds(eb) for eb in obj.execution_bounds]
        return TaskRun(id=obj.id,
                       task_id=obj.task_id,
                       group_name=obj.group_name,
                       priority=obj.priority,
                       type=obj.type,
                       payload=payload,
                       execution_bounds=execution_bounds,
                       execution_arguments=obj.execution_arguments,
                       status=obj.status,
                       status_updated_at=obj.status_updated_at,
                       description=obj.description,
                       )

    def pk_to_model_pk(self, pk: TaskRunPK) -> Dict:
        return {"id": pk.id}

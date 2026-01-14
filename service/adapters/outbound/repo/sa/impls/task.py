from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.impls.task_mapper import TaskMapper
from service.domain.schemas.task import TaskPK, Task


class SATaskRepo(AbstractSARepo):
    def to_model(self, obj: Task) -> models.Task:
        return TaskMapper.to_model(obj)

    def to_domain(self, obj: models.Task) -> Task:
        return TaskMapper.to_domain(obj)

    def pk_to_model_pk(self, pk: TaskPK) -> Dict:
        return {"id": pk.id}

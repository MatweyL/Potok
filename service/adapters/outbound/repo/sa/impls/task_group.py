from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK


class SATaskGroupRepo(AbstractSARepo):
    def to_model(self, obj: TaskGroup) -> models.TaskGroup:
        return models.TaskGroup(id=obj.id,
                                name=obj.name,
                                title=obj.title,
                                description=obj.description,
                                is_active=obj.is_active, )

    def to_domain(self, obj: models.TaskGroup) -> TaskGroup:
        return TaskGroup(id=obj.id,
                         name=obj.name,
                         title=obj.title,
                         description=obj.description,
                         is_active=obj.is_active, )

    def pk_to_model_pk(self, pk: TaskGroupPK) -> Dict:
        return {"id": pk.id}

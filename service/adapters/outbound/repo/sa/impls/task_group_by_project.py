from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task_group_by_project import TaskGroupByProject, TaskGroupByProjectPK


class SATaskGroupByProjectRepo(AbstractSARepo):
    def to_model(self, obj: TaskGroupByProject) -> models.TaskGroupByProject:
        return models.TaskGroupByProject(group_id=obj.group_id,
                                         project_id=obj.project_id)

    def to_domain(self, obj: models.TaskGroupByProject) -> TaskGroupByProject:
        return TaskGroupByProject(group_id=obj.group_id,
                                  project_id=obj.project_id)

    def pk_to_model_pk(self, pk: TaskGroupByProjectPK) -> Dict:
        return {"group_id": pk.group_id,
                "project_id": pk.project_id}

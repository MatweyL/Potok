from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task import TaskStatusLog, TaskStatusLogPK


class SATaskStatusLogRepo(AbstractSARepo):
    def to_model(self, obj: TaskStatusLog) -> models.TaskStatusLog:
        return models.TaskStatusLog(task_id=obj.task_id,
                                    status_updated_at=obj.status_updated_at,
                                    status=obj.status,
                                    description=obj.description)

    def to_domain(self, obj: models.TaskStatusLog) -> TaskStatusLog:
        return TaskStatusLog(task_id=obj.task_id,
                             status_updated_at=obj.status_updated_at,
                             status=obj.status,
                             description=obj.description)

    def pk_to_model_pk(self, pk: TaskStatusLogPK) -> Dict:
        return {
            "task_id": pk.task_id,
            "status_updated_at": pk.status_updated_at
        }

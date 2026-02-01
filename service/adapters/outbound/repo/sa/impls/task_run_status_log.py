from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task_run import TaskRunStatusLog, TaskRunStatusLogPK


class SATaskRunStatusLogRepo(AbstractSARepo):
    def to_model(self, obj: TaskRunStatusLog) -> models.TaskRunStatusLog:
        return models.TaskRunStatusLog(task_run_id=obj.task_run_id,
                                       status_updated_at=obj.status_updated_at,
                                       status=obj.status,
                                       description=obj.description)

    def to_domain(self, obj: models.TaskRunStatusLog) -> TaskRunStatusLog:
        return TaskRunStatusLog(task_run_id=obj.task_run_id,
                                status_updated_at=obj.status_updated_at,
                                status=obj.status,
                                description=obj.description)

    def pk_to_model_pk(self, pk: TaskRunStatusLogPK) -> Dict:
        return {
            "task_run_id": pk.task_run_id,
            "status_updated_at": pk.status_updated_at
        }

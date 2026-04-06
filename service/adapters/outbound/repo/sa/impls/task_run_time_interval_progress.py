from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task_run import TaskRunTimeIntervalProgress, TaskRunTimeIntervalProgressPK


class SATaskRunTimeIntervalProgressRepo(AbstractSARepo):
    def to_model(self, obj: TaskRunTimeIntervalProgress) -> models.TaskRunTimeIntervalProgress:
        return models.TaskRunTimeIntervalProgress(task_run_id=obj.task_run_id,
                                                  right_bound_at=obj.right_bound_at,
                                                  left_bound_at=obj.left_bound_at,
                                                  collected_data_amount=obj.collected_data_amount,
                                                  saved_data_amount=obj.saved_data_amount)

    def to_domain(self, obj: models.TaskRunTimeIntervalProgress) -> TaskRunTimeIntervalProgress:
        return TaskRunTimeIntervalProgress(task_run_id=obj.task_run_id,
                                           right_bound_at=obj.right_bound_at,
                                           left_bound_at=obj.left_bound_at,
                                           collected_data_amount=obj.collected_data_amount,
                                           saved_data_amount=obj.saved_data_amount)

    def pk_to_model_pk(self, pk: TaskRunTimeIntervalProgressPK) -> Dict:
        return {
            "task_run_id": pk.task_run_id,
            "right_bound_at": pk.right_bound_at
        }

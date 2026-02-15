from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task_run import TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK


class SATaskRunTimeIntervalExecutionBoundsRepo(AbstractSARepo):
    def to_model(self, obj: TaskRunTimeIntervalExecutionBounds) -> models.TaskRunTimeIntervalExecutionBounds:
        return models.TaskRunTimeIntervalExecutionBounds(task_run_id=obj.task_run_id,
                                                         task_id=obj.task_id,
                                                         right_bound_at=obj.execution_bounds.right_bound_at,
                                                         left_bound_at=obj.execution_bounds.left_bound_at,
                                                         )

    def to_domain(self, obj: models.TaskRunTimeIntervalExecutionBounds) -> TaskRunTimeIntervalExecutionBounds:
        return TaskRunTimeIntervalExecutionBounds(task_run_id=obj.task_run_id,
                                                  task_id=obj.task_id,
                                                  execution_bounds=TimeIntervalBounds(
                                                      right_bound_at=obj.right_bound_at,
                                                      left_bound_at=obj.left_bound_at,
                                                  ))

    def pk_to_model_pk(self, pk: TaskRunTimeIntervalExecutionBoundsPK) -> Dict:
        return {
            "task_run_id": pk.task_run_id,
        }

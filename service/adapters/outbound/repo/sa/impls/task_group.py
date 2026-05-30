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
                                is_active=obj.is_active,
                                queue_per_priority=obj.queue_per_priority,
                                execution_arguments=obj.execution_arguments,
                                time_interval_max_period=obj.time_interval_max_period,
                                time_interval_first_left_bound_at=obj.time_interval_first_left_bound_at,
                                time_interval_first_left_bound_depth=obj.time_interval_first_left_bound_depth, )

    def to_domain(self, obj: models.TaskGroup) -> TaskGroup:
        return TaskGroup(id=obj.id,
                         name=obj.name,
                         title=obj.title,
                         description=obj.description,
                         is_active=obj.is_active,
                         queue_per_priority=obj.queue_per_priority,
                         execution_arguments=obj.execution_arguments,
                         time_interval_max_period=obj.time_interval_max_period,
                         time_interval_first_left_bound_at=obj.time_interval_first_left_bound_at,
                         time_interval_first_left_bound_depth=obj.time_interval_first_left_bound_depth, )

    def pk_to_model_pk(self, pk: TaskGroupPK) -> Dict:
        return {"id": pk.id}

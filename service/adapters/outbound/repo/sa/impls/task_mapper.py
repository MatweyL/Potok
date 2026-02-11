from service.adapters.outbound.repo.sa import models
from service.domain.schemas.task import Task


class TaskMapper:
    """ Код маппинга вынесен в отдельный класс т.к. он используется в нескольких местах """

    @staticmethod
    def to_model(obj: Task) -> models.Task:
        return models.Task(id=obj.id,
                           group_name=obj.group_name,
                           priority=obj.priority,
                           type=obj.type,
                           monitoring_algorithm_id=obj.monitoring_algorithm_id,
                           execution_arguments=obj.execution_arguments,
                           status=obj.status,
                           status_updated_at=obj.status_updated_at,
                           payload_id=obj.payload_id,
                           loaded_at=obj.loaded_at, )

    @staticmethod
    def to_domain(obj: models.Task) -> Task:
        return Task(id=obj.id,
                    group_name=obj.group_name,
                    priority=obj.priority,
                    type=obj.type,
                    monitoring_algorithm_id=obj.monitoring_algorithm_id,
                    execution_arguments=obj.execution_arguments,
                    status=obj.status,
                    status_updated_at=obj.status_updated_at,
                    payload_id=obj.payload_id,
                    loaded_at=obj.loaded_at, )

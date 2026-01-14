from datetime import datetime
from typing import Dict, List

from sqlalchemy import select, text, DateTime, func
from sqlalchemy.sql.operators import eq, lt, and_

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.impls.task_mapper import TaskMapper
from service.domain.schemas.enums import MonitoringAlgorithmType, TaskStatus
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmPK, MonitoringAlgorithm, \
    PeriodicMonitoringAlgorithm
from service.domain.schemas.task import Task
from service.ports.outbound.repo.monitoring_algorithm import MonitoringAlgorithmRepo


class SAMonitoringAlgorithmRepo(AbstractSARepo):
    def to_model(self, obj: MonitoringAlgorithm) -> models.MonitoringAlgorithm:
        return models.MonitoringAlgorithm(id=obj.id, type=obj.type)

    def to_domain(self, obj: models.MonitoringAlgorithm) -> MonitoringAlgorithm:
        return MonitoringAlgorithm(id=obj.id, type=obj.type)

    def pk_to_model_pk(self, pk: MonitoringAlgorithmPK) -> Dict:
        return {'id': pk.id}


class SAPeriodicMonitoringAlgorithmRepo(AbstractSARepo, MonitoringAlgorithmRepo):
    def to_model(self, obj: PeriodicMonitoringAlgorithm) -> models.PeriodicMonitoringAlgorithm:
        return models.PeriodicMonitoringAlgorithm(id=obj.id,
                                                  timeout=obj.timeout,
                                                  timeout_noize=obj.timeout_noize)

    def to_domain(self, obj: models.PeriodicMonitoringAlgorithm) -> PeriodicMonitoringAlgorithm:
        return PeriodicMonitoringAlgorithm(id=obj.id,
                                           type=MonitoringAlgorithmType.PERIODIC,
                                           timeout=obj.timeout,
                                           timeout_noize=obj.timeout_noize)

    def pk_to_model_pk(self, pk: MonitoringAlgorithmPK) -> Dict:
        return {'id': pk.id}

    async def provide_tasks_to_execute(self) -> List[Task]:
        async with self._database.session as session:
            current_datetime = datetime.now()
            ready_to_execute_by_timeout = lt(func.cast(text("task.status_updated_at + "
                                                            " (interval '1' second * periodic_monitoring_algorithm.timeout)"),
                                                       DateTime(timezone=False)),
                                             current_datetime)
            query = (
                select(models.Task)
                .join(self._model_class,
                      onclause=eq(models.Task.monitoring_algorithm_id,
                                  self._model_class.id))
                .where((
                        (models.Task.status == TaskStatus.NEW) |
                        and_(models.Task.status == TaskStatus.EXECUTION,
                             ready_to_execute_by_timeout) |
                        and_(models.Task.status == TaskStatus.SUCCEED,
                             ready_to_execute_by_timeout)
                ))
            )
            result = await session.scalars(query, )
            tasks = result.all()
            return [TaskMapper.to_domain(task) for task in tasks]

import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from sqlalchemy import select, text, DateTime, func
from sqlalchemy.sql.operators import eq, lt, and_

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.impls.task_mapper import TaskMapper
from service.domain.schemas.enums import MonitoringAlgorithmType, TaskStatus
from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmPK, MonitoringAlgorithm, \
    PeriodicMonitoringAlgorithm, SingleMonitoringAlgorithm
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


class SASingleMonitoringAlgorithmRepo(AbstractSARepo, MonitoringAlgorithmRepo):
    def to_model(self, obj: PeriodicMonitoringAlgorithm) -> models.SingleMonitoringAlgorithm:
        return models.SingleMonitoringAlgorithm(id=obj.id,
                                                timeouts=obj.timeouts,
                                                timeout_noize=obj.timeout_noize)

    def to_domain(self, obj: models.SingleMonitoringAlgorithm) -> SingleMonitoringAlgorithm:
        return SingleMonitoringAlgorithm(id=obj.id,
                                         type=MonitoringAlgorithmType.SINGLE,
                                         timeouts=obj.timeouts,
                                         timeout_noize=obj.timeout_noize)

    def pk_to_model_pk(self, pk: MonitoringAlgorithmPK) -> Dict:
        return {'id': pk.id}

    async def provide_tasks_to_execute(self) -> List[Task]:
        """
        Возвращает задачи, готовые к выполнению для SingleMonitoringAlgorithm.

        Логика:
        1. Для каждой задачи вычисляем интервалы выполнения на основе loaded_at + timeouts
        2. Находим текущий интервал: left_bound <= now < right_bound
        3. Задача готова к выполнению, если:
           - status == NEW, или
           - status == SUCCEED и status_updated_at < left_bound текущего интервала
        """
        async with self._database.session as session:
            # Получаем все задачи с SingleMonitoringAlgorithm
            query = (
                select(models.Task, self._model_class)
                .join(
                    self._model_class,
                    onclause=eq(models.Task.monitoring_algorithm_id, self._model_class.id),
                )
            )
            result = await session.execute(query)
            rows = result.all()

            now = datetime.now()
            ready_tasks = []

            for task_model, algorithm_model in rows:
                task = TaskMapper.to_domain(task_model)
                algorithm = self.to_domain(algorithm_model)

                # Проверяем, готова ли задача к выполнению
                if self._is_task_ready_to_execute(task, algorithm, now):
                    ready_tasks.append(task)

            return ready_tasks

    def _is_task_ready_to_execute(
            self, task: Task, algorithm: SingleMonitoringAlgorithm, now: datetime
    ) -> bool:
        """
        Проверяет, готова ли задача к выполнению в текущий момент.
        """
        # Вычисляем интервалы выполнения
        intervals = self._calculate_execution_intervals(task, algorithm)

        # Находим текущий интервал
        current_interval = self._find_current_interval(intervals, now)

        if current_interval is None:
            # Нет текущего интервала — задача не готова
            return False

        left_bound, right_bound = current_interval

        # Проверяем условия готовности
        if task.status == TaskStatus.NEW:
            return True

        if task.status == TaskStatus.SUCCEED and task.status_updated_at < left_bound:
            return True

        return False

    def _calculate_execution_intervals(
            self, task: Task, algorithm: SingleMonitoringAlgorithm
    ) -> List[Tuple[datetime, datetime]]:
        """
        Вычисляет интервалы выполнения для задачи.

        Если timeouts пустой — одно выполнение сразу после loaded_at.
        Если timeouts = [t1, t2, t3] — задача выполнится 4 раза:
          - Интервал 0: [loaded_at, loaded_at + t1 + noise]
          - Интервал 1: [loaded_at + t1 + noise, loaded_at + t1 + t2 + noise]
          - и т.д.
        """
        intervals = []
        loaded_at = task.loaded_at

        if not algorithm.timeouts:
            # Одно выполнение: интервал от loaded_at до бесконечности
            intervals.append((loaded_at, datetime.max))
            return intervals

        cumulative_time = loaded_at

        for timeout in algorithm.timeouts:
            left_bound = cumulative_time

            # Добавляем noise (случайное отклонение ±timeout_noize)
            noise = random.uniform(-algorithm.timeout_noize, algorithm.timeout_noize)
            actual_timeout = timeout + noise

            right_bound = cumulative_time + timedelta(seconds=actual_timeout)
            intervals.append((left_bound, right_bound))

            cumulative_time = right_bound

        # Последний интервал — после всех timeouts до бесконечности
        intervals.append((cumulative_time, datetime.max))

        return intervals

    def _find_current_interval(
            self, intervals: List[Tuple[datetime, datetime]], now: datetime
    ) -> Optional[Tuple[datetime, datetime]]:
        """
        Находит интервал, в котором left_bound <= now < right_bound.
        """
        for left, right in intervals:
            if left <= now < right:
                return (left, right)
        return None

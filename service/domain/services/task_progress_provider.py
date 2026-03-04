from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any

from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task_progress import TimeIntervalTaskProgress, TimeIntervalTaskProgressPK
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


class TimeInterval:
    def __init__(self, left_bound_at: datetime, right_bound_at: datetime):
        self.left_bound_at = left_bound_at
        self.right_bound_at = right_bound_at

    def has_interval(self, left_bound: datetime, right_bound: datetime) -> bool:
        return self.has_date(left_bound) and self.has_date(right_bound)

    def is_interval_lower(self, left_bound: datetime, right_bound: datetime) -> bool:
        return self.is_date_lower(left_bound) and self.is_date_lower(right_bound)

    def is_interval_greater(self, left_bound: datetime, right_bound: datetime) -> bool:
        return self.is_date_greater(left_bound) and self.is_date_greater(right_bound)

    def has_date(self, dt: datetime) -> bool:
        """ Определяет, содержится ли дата в указанном интервале"""
        return self.left_bound_at <= dt <= self.right_bound_at

    def is_date_greater(self, dt: datetime) -> bool:
        """ Определяет, является ли дата больше правой границы интервала """
        return dt > self.right_bound_at

    def is_date_lower(self, dt: datetime) -> bool:
        return dt < self.left_bound_at


class TimeIntervalExecutionBoundsCutter:
    def __init__(self, time_interval_task_progresses: List[TimeIntervalTaskProgress]):
        self.intervals = self.create_intervals(time_interval_task_progresses)

    def create_intervals(self, time_interval_task_progresses: List[TimeIntervalTaskProgress]):
        if time_interval_task_progresses:
            time_interval_task_progresses.sort(key=lambda obj: obj.right_bound_at, reverse=True)
            previous_interval = TimeInterval(time_interval_task_progresses[0].left_bound_at,
                                             time_interval_task_progresses[0].right_bound_at, )
            intervals = [previous_interval]
            for time_interval_task_progress in time_interval_task_progresses[1:]:
                right_bound_at_in_time_interval = previous_interval.has_date(time_interval_task_progress.right_bound_at)
                left_bound_at_in_time_interval = previous_interval.has_date(time_interval_task_progress.left_bound_at)
                if right_bound_at_in_time_interval and left_bound_at_in_time_interval:
                    # Интервал полностью находится в предыдущем интервале
                    continue
                elif right_bound_at_in_time_interval:
                    # Правая граница интервала принадлежит предыдущему - обновляем левую границу на минимальную
                    previous_interval.left_bound_at = min(time_interval_task_progress.left_bound_at,
                                                       previous_interval.left_bound_at)
                else:
                    # Правая граница не принадлежит предыдущему - создаем новый объект
                    previous_interval = TimeInterval(time_interval_task_progress.left_bound_at,
                                                     time_interval_task_progress.right_bound_at)
                    intervals.append(previous_interval)
            return intervals

        return []

    def cut(self, execution_bounds: TimeIntervalBounds):
        if not self.intervals:
            return execution_bounds
        for interval in self.intervals:
            if interval.has_interval(execution_bounds.left_bound_at, execution_bounds.right_bound_at):
                # Граничный случай: интервал уже выполнен, возвращаем равные даты в границах выполнения
                execution_bounds.left_bound_at = execution_bounds.right_bound_at
                return execution_bounds
            if interval.is_interval_lower(execution_bounds.left_bound_at, execution_bounds.right_bound_at):
                # Интервал полностью меньше - идем дальше
                continue
            # Иначе - правая граница входит в этот интервал. Левая граница задачи всегда постоянна
            # из-за атомарности запуска задач
            execution_bounds.right_bound_at = interval.left_bound_at
            return execution_bounds
        return execution_bounds


class ActualExecutionBoundsProvider(ABC):

    @abstractmethod
    async def provide(self, tasks_ids: List[int]) -> Dict[int, TimeIntervalExecutionBoundsCutter]:
        pass


class ActualTimeIntervalExecutionBoundsProvider(ActualExecutionBoundsProvider):
    def __init__(self, time_interval_task_progress_repo: Repo[TimeIntervalTaskProgress,
                                                              TimeIntervalTaskProgress,
                                                              TimeIntervalTaskProgressPK],
                 ):
        self._time_interval_task_progress_repo = time_interval_task_progress_repo

    async def provide(self, tasks_ids: List[int]) -> Dict[int, TimeIntervalExecutionBoundsCutter]:
        time_interval_task_progresses = await self._time_interval_task_progress_repo.filter(
            FilterFieldsDNF.single('task_id', tasks_ids, ConditionOperation.IN)
        )
        titp_list_by_task_id = defaultdict(list)
        for time_interval_task_progress in time_interval_task_progresses:
            titp_list_by_task_id[time_interval_task_progress.task_id].append(time_interval_task_progress)
        cutter_by_task_id = {task_id: TimeIntervalExecutionBoundsCutter(titp_list)
                             for task_id, titp_list in titp_list_by_task_id.items()}
        return cutter_by_task_id

from datetime import datetime, timezone
from typing import Dict, List

from sqlalchemy import func, select, text

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.database import Database
from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task_run import TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK
from service.ports.outbound.repo.task_run import LatestTaskRunTimeIntervalExecutionBoundsProvider


class TaskRunTimeIntervalExecutionBoundsMapper:
    @staticmethod
    def to_model(obj: TaskRunTimeIntervalExecutionBounds) -> models.TaskRunTimeIntervalExecutionBounds:
        return models.TaskRunTimeIntervalExecutionBounds(task_run_id=obj.task_run_id,
                                                         task_id=obj.task_id,
                                                         right_bound_at=obj.execution_bounds.right_bound_at,
                                                         left_bound_at=obj.execution_bounds.left_bound_at,
                                                         )

    @staticmethod
    def to_domain(obj: models.TaskRunTimeIntervalExecutionBounds) -> TaskRunTimeIntervalExecutionBounds:
        return TaskRunTimeIntervalExecutionBounds(task_run_id=obj.task_run_id,
                                                  task_id=obj.task_id,
                                                  execution_bounds=TimeIntervalBounds(
                                                      right_bound_at=obj.right_bound_at,
                                                      left_bound_at=obj.left_bound_at,
                                                  ))


class SATaskRunTimeIntervalExecutionBoundsRepo(AbstractSARepo):
    def to_model(self, obj: TaskRunTimeIntervalExecutionBounds) -> models.TaskRunTimeIntervalExecutionBounds:
        return TaskRunTimeIntervalExecutionBoundsMapper.to_model(obj)

    def to_domain(self, obj: models.TaskRunTimeIntervalExecutionBounds) -> TaskRunTimeIntervalExecutionBounds:
        return TaskRunTimeIntervalExecutionBoundsMapper.to_domain(obj)

    def pk_to_model_pk(self, pk: TaskRunTimeIntervalExecutionBoundsPK) -> Dict:
        return {
            "task_run_id": pk.task_run_id,
        }

    async def get_latest_right_bound_by_task_ids(self, task_ids: List[int]) -> Dict[int, datetime]:
        if not task_ids:
            return {}
        query = (
            select(
                self._model_class.task_id,
                func.max(self._model_class.right_bound_at).label("right_bound_at"),
            )
            .where(self._model_class.task_id.in_(task_ids))
            .group_by(self._model_class.task_id)
        )
        async with self._database.session as session:
            result = await session.execute(query)
            return {task_id: right_bound_at for task_id, right_bound_at in result.all() if right_bound_at}


class SALatestTaskRunTimeIntervalExecutionBoundsProvider(LatestTaskRunTimeIntervalExecutionBoundsProvider):
    def __init__(self, database: Database, ):
        self._database = database

    async def provide_latest_bounds_by_task_ids(self, task_ids: List[int]) -> Dict[
        int, TaskRunTimeIntervalExecutionBounds]:
        async with self._database.session as session:  # FIXME: проверить, корректно ли делать where task_id = ANY(:task_ids) вмето task IN ...
            query = text("""              
    with task_last_right_bound as (
        select task_id, max(right_bound_at) as right_bound_at from task_run_time_interval_execution_bounds
        where task_id = ANY(:task_ids)
        group by 1
    )
    select trtieb1.* from task_run_time_interval_execution_bounds trtieb1
    join task_last_right_bound tlrb on tlrb.task_id = trtieb1.task_id and tlrb.right_bound_at = trtieb1.right_bound_at;
                   """)
            result = await session.execute(query, {
                "task_ids": task_ids
            })
            rows = result.mappings().all()
            return {row["task_id"]: TaskRunTimeIntervalExecutionBoundsMapper.to_domain(row) for row in rows}


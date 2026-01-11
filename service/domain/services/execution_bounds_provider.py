from datetime import datetime, timedelta
from typing import Dict, List, Protocol, runtime_checkable

from service.domain.schemas.enums import TaskType
from service.domain.schemas.execution_bounds import ExecutionBounds, TimeIntervalBounds
from service.domain.schemas.task import Task
from service.domain.schemas.task_progress import (
    TimeIntervalTaskProgress,
    TimeIntervalTaskProgressPK,
)
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


@runtime_checkable
class ExecutionBoundsProvider(Protocol):
    async def provide_batch(self, tasks: List[Task]) -> Dict[Task, List[ExecutionBounds]]:
        """
        Для каждой задачи возвращает список границ выполнения (execution_bounds).
        Ключ — сама задача (Task), значение — список ExecutionBounds (может быть пустым).
        """
        ...


class DefaultExecutionBoundsProvider:
    """
    Провайдер границ выполнения в зависимости от типа задачи.
    Сейчас поддерживает только TIME_INTERVAL, легко расширяется.
    """

    def __init__(
            self,
            time_interval_progress_repo: Repo[
                TimeIntervalTaskProgress, TimeIntervalTaskProgress, TimeIntervalTaskProgressPK
            ],
            default_left_date: datetime = None,
            default_first_interval_days: int = 31,
    ):
        self._time_interval_progress_repo = time_interval_progress_repo
        self._default_left_date = default_left_date or datetime(2010, 1, 1)
        self._default_first_interval_days = default_first_interval_days

        # Можно добавить другие репозитории по мере появления новых типов задач
        # self._pagination_progress_repo = pagination_progress_repo

    async def provide_batch(self, tasks: List[Task]) -> Dict[Task, List[ExecutionBounds]]:
        result: Dict[Task, List[ExecutionBounds]] = {}

        # Группируем задачи по типу для оптимизации запросов
        time_interval_tasks = [task for task in tasks if task.type == TaskType.TIME_INTERVAL]
        # pagination_tasks = [task for task in tasks if task.type == "PAGINATION"]
        # другие типы...

        # Обрабатываем TIME_INTERVAL задачи
        if time_interval_tasks:
            time_interval_bounds = await self._provide_time_interval_bounds(time_interval_tasks)
            result.update(time_interval_bounds)

        # Здесь можно добавить обработку других типов
        # if pagination_tasks:
        #     pagination_bounds = await self._provide_pagination_bounds(pagination_tasks)
        #     result.update(pagination_bounds)

        # Для неподдерживаемых типов или задач без прогресса — пустой список
        for task in tasks:
            if task not in result:
                result[task] = []

        return result

    async def _provide_time_interval_bounds(
            self, tasks: List[Task]
    ) -> Dict[Task, List[ExecutionBounds]]:
        """
        Логика для задач типа TIME_INTERVAL:
        - Ищем последний завершённый интервал (максимальный right_bound_at)
        - Формируем следующий интервал: от right_bound_at до now (или до конфигурационного шага)
        - Если прогресса нет — начинаем с самого начала (например, с 1970 или из конфига)
        """

        task_ids = [task.id for task in tasks]
        progress_records: List[TimeIntervalTaskProgress] = await self._time_interval_progress_repo.filter(
            FilterFieldsDNF.single("task_id", task_ids, ConditionOperation.IN)
        )

        # Группируем прогресс по task_id
        progress_by_task_id: Dict[int, List[TimeIntervalTaskProgress]] = {}
        for record in progress_records:
            progress_by_task_id.setdefault(record.task_id, []).append(record)

        result: Dict[Task, List[ExecutionBounds]] = {}

        now = datetime.now()

        for task in tasks:
            task_progress = progress_by_task_id.get(task.id, [])

            if not task_progress:
                # Нет прогресса — нужно создать первый интервал.
                # Здесь ты можешь задать стартовую точку из конфига задачи (execution_arguments?)
                # Для примера — начинаем с None (т.е. с самого начала)
                separate_monitoring_and_retro_datetime = now - timedelta(days=self._default_first_interval_days)
                bounds = [TimeIntervalBounds(right_bound_at=now,
                                             left_bound_at=separate_monitoring_and_retro_datetime),
                          TimeIntervalBounds(right_bound_at=separate_monitoring_and_retro_datetime,
                                             left_bound_at=self._default_left_date)]
                result[task] = bounds
                continue

            # Находим последний завершённый интервал
            latest_progress = max(task_progress, key=lambda p: p.right_bound_at)

            # Предполагаем, что интервал завершён, если collected_data_amount == saved_data_amount
            # Или просто берём последний по времени — зависит от твоей бизнес-логики
            if latest_progress.collected_data_amount == latest_progress.saved_data_amount:
                # Можно продолжать — следующий интервал начинается после latest.right_bound_at
                next_left = latest_progress.right_bound_at
            else:
                # Последний интервал не завершён — возможно, нужно его перезапустить?
                # Или пропустить? Зависит от требований.
                # Пока просто создаём новый после него
                next_left = latest_progress.right_bound_at

            bounds = [TimeIntervalBounds(right_bound_at=now, left_bound_at=next_left)]
            result[task] = bounds

        return result

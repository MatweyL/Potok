from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from more_itertools import batched
from sqlalchemy.util import await_only

from service.domain.schemas.enums import TaskStatus, TaskRunStatus, TaskType
from service.domain.schemas.execution_bounds import ExecutionBounds
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import TaskPK, Task, TaskStatusLog, TaskStatusLogPK
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_run import (
    TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK,
    TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK,
)
from service.domain.services.payload_provider import PayloadProvider
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields, FilterFieldsDNF, ConditionOperation, PaginationQuery
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.ports.outbound.repo.task_run import LatestTaskRunTimeIntervalExecutionBoundsProvider
from service.ports.outbound.repo.transaction import TransactionFactory


class CreateTaskRunsUCRq(UCRequest):
    pass


class CreateTaskRunsUCRs(UCResponse):
    request: CreateTaskRunsUCRq
    task_runs_created: int


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные структуры
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TimeInterval:
    left_bound_at:  datetime
    right_bound_at: datetime

    @property
    def duration_seconds(self) -> float:
        return (self.right_bound_at - self.left_bound_at).total_seconds()


# ─────────────────────────────────────────────────────────────────────────────
# Обработчики задач по типу
# ─────────────────────────────────────────────────────────────────────────────

class UndefinedTaskRunBuilder:
    """Для UNDEFINED-задач: один запуск без границ."""

    def build(
        self,
        task:      Task,
        group:     TaskGroup,
        payload:   Payload,
        now:       datetime,
    ) -> List[TaskRun]:
        return [TaskRun(
            task_id=task.id,
            group_name=group.name,
            priority=task.priority,
            type=task.type,
            payload=payload,
            execution_bounds=None,
            execution_arguments=task.execution_arguments,
            status=TaskRunStatus.WAITING,
            status_updated_at=now,
        )]


class TimeIntervalTaskRunBuilder:
    """
    Алгоритм создания запусков для задач типа TIME_INTERVAL.

    При наличии last_bounds берёт: left = last.right_bound_at, right = now.
    При отсутствии — вычисляет первый left из TaskGroup:
        time_interval_first_left_bound_at  — абсолютная дата
        time_interval_first_left_bound_depth — относительная глубина (секунды)

    Если задан time_interval_max_period и интервал его превышает —
    делит на равные отрезки.
    """

    def build(
        self,
        task:        Task,
        group:       TaskGroup,
        payload:     Payload,
        now:         datetime,
        last_bounds: Optional[TaskRunTimeIntervalExecutionBounds],
    ) -> List[TimeInterval]:
        """Возвращает список временных интервалов для создания TaskRun."""

        if last_bounds is not None:
            left  = last_bounds.execution_bounds.right_bound_at
            right = now
        else:
            left  = self._first_left_bound(group, now)
            right = now

        if left >= right:
            return []

        interval = TimeInterval(left_bound_at=left, right_bound_at=right)
        max_period = group.time_interval_max_period  # секунды или None

        if max_period is None or interval.duration_seconds <= max_period:
            return [interval]

        return self._split(interval, max_period)

    @staticmethod
    def _first_left_bound(group: TaskGroup, now: datetime) -> datetime:
        if group.time_interval_first_left_bound_at is not None:
            return group.time_interval_first_left_bound_at
        depth = group.time_interval_first_left_bound_depth
        if depth is None:
            raise ValueError(
                f"TaskGroup {group.id}: нужно задать "
                "time_interval_first_left_bound_at или time_interval_first_left_bound_depth"
            )
        return now - timedelta(seconds=depth)

    @staticmethod
    def _split(interval: TimeInterval, max_period_seconds: float) -> List[TimeInterval]:
        """Делит интервал на отрезки длиной не более max_period_seconds."""
        runs_number = int(interval.duration_seconds // max_period_seconds) + 1
        step = timedelta(seconds=interval.duration_seconds / runs_number)

        result: List[TimeInterval] = []
        left = interval.left_bound_at
        for _ in range(runs_number):
            right = min(left + step, interval.right_bound_at)
            if right > left:
                result.append(TimeInterval(left_bound_at=left, right_bound_at=right))
            left = right

        return result

    def build_task_runs(
        self,
        task:        Task,
        group:       TaskGroup,
        payload:     Payload,
        now:         datetime,
        last_bounds: Optional[TaskRunTimeIntervalExecutionBounds],
    ) -> List[TaskRun]:
        intervals = self.build(task, group, payload, now, last_bounds)
        return [
            TaskRun(
                task_id=task.id,
                group_name=group.name,
                priority=task.priority,
                type=task.type,
                payload=payload,
                execution_bounds=ExecutionBounds(
                    left_bound_at=iv.left_bound_at,
                    right_bound_at=iv.right_bound_at,
                ),
                execution_arguments=task.execution_arguments,
                status=TaskRunStatus.WAITING,
                status_updated_at=now,
            )
            for iv in intervals
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Use Case
# ─────────────────────────────────────────────────────────────────────────────

class CreateTaskRunsUC(UseCase):

    def __init__(
        self,
        task_repo:                                    Repo[Task,         Task,         TaskPK],
        task_run_repo:                                Repo[TaskRun,      TaskRun,      TaskRunPK],
        task_status_log_repo:                         Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK],
        task_run_status_log_repo:                     Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
        task_run_time_interval_execution_bounds_repo: Repo[
            TaskRunTimeIntervalExecutionBounds,
            TaskRunTimeIntervalExecutionBounds,
            TaskRunTimeIntervalExecutionBoundsPK,
        ],
        transaction_factory:                          TransactionFactory,
        tasks_to_execute_provider_registry:           TaskToExecuteProviderRegistry,
        payload_provider:                             PayloadProvider,
        task_group_repo:                              Repo[TaskGroup, TaskGroup, TaskGroupPK],
        latest_task_run_time_interval_execution_bounds_provider: LatestTaskRunTimeIntervalExecutionBoundsProvider,
        tasks_batch_size:                             int = 5000,
    ):
        self._task_repo                                    = task_repo
        self._task_run_repo                                = task_run_repo
        self._task_status_log_repo                         = task_status_log_repo
        self._task_run_status_log_repo                     = task_run_status_log_repo
        self._task_run_time_interval_execution_bounds_repo = task_run_time_interval_execution_bounds_repo
        self._transaction_factory                          = transaction_factory
        self._tasks_to_execute_provider_registry           = tasks_to_execute_provider_registry
        self._payload_provider                             = payload_provider
        self._task_group_repo                              = task_group_repo
        self._latest_task_run_time_interval_execution_bounds_provider = latest_task_run_time_interval_execution_bounds_provider
        self._tasks_batch_size                             = tasks_batch_size

        self._undefined_builder     = UndefinedTaskRunBuilder()
        self._time_interval_builder = TimeIntervalTaskRunBuilder()

    async def apply(self, request: CreateTaskRunsUCRq) -> CreateTaskRunsUCRs:
        tasks = await self._tasks_to_execute_provider_registry.provide_tasks_to_execute()
        if not tasks:
            return CreateTaskRunsUCRs(success=True, request=request, task_runs_created=0)

        total_created = 0
        async with self._transaction_factory.create() as transaction:
            for chunk in batched(tasks, self._tasks_batch_size):
                chunk = list(chunk)
                created = await self._process_chunk(chunk, transaction)
                total_created += created

        logger.info(f"CreateTaskRunsUC: создано {total_created} запусков")
        return CreateTaskRunsUCRs(success=True, request=request, task_runs_created=total_created)

    async def _process_chunk(self, tasks: List[Task], transaction) -> int:
        now = datetime.now(timezone.utc)

        # ── 1. Загружаем группы и payload батчем ─────────────────────────────
        group_ids  = list({t.group_id for t in tasks})
        task_groups = await self._task_group_repo.filter(
            FilterFieldsDNF.single("id", group_ids, ConditionOperation.IN)
        )
        group_by_id: Dict[int, TaskGroup] = {g.id: g for g in task_groups}
        payload_by_task: Dict[Task, Payload] = await self._payload_provider.provide(tasks)

        # ── 2. Разделяем задачи по типу ──────────────────────────────────────
        undefined_tasks:     List[Task] = []
        time_interval_tasks: List[Task] = []

        for task in tasks:
            if task.type == TaskType.TIME_INTERVAL:
                time_interval_tasks.append(task)
            else:
                undefined_tasks.append(task)

        # ── 3. Строим TaskRun для каждого типа ───────────────────────────────
        all_task_runs: List[TaskRun] = []

        # UNDEFINED — просто создаём по одному запуску
        for task in undefined_tasks:
            group   = group_by_id[task.group_id]
            payload = payload_by_task[task]
            all_task_runs.extend(
                self._undefined_builder.build(task, group, payload, now)
            )

        # TIME_INTERVAL — нужна последняя запись bounds для каждой задачи
        if time_interval_tasks:
            last_bounds_by_task_id = await self._load_last_bounds(time_interval_tasks)
            for task in time_interval_tasks:
                group       = group_by_id[task.group_id]
                payload     = payload_by_task[task]
                last_bounds = last_bounds_by_task_id.get(task.id)
                runs = self._time_interval_builder.build_task_runs(
                    task, group, payload, now, last_bounds
                )
                all_task_runs.extend(runs)

        if not all_task_runs:
            return 0

        # ── 4. Сохраняем всё в одной транзакции ──────────────────────────────
        task_ids_with_runs = {run.task_id for run in all_task_runs}
        tasks_to_update    = [t for t in tasks if t.id in task_ids_with_runs]

        await self._task_repo.update_all(
            {
                task: UpdateFields.multiple({
                    "status":            TaskStatus.EXECUTION,
                    "status_updated_at": now,
                })
                for task in tasks_to_update
            },
            transaction=transaction,
        )

        task_runs_created = await self._task_run_repo.create_all(all_task_runs, transaction=transaction)

        # Логи статусов задач и запусков
        await self._task_status_log_repo.create_all(
            [TaskStatusLog(task_id=t.id, status_updated_at=now, status=TaskStatus.EXECUTION)
             for t in tasks_to_update],
            transaction=transaction,
        )
        await self._task_run_status_log_repo.create_all(
            [TaskRunStatusLog(task_run_id=r.id, status_updated_at=now, status=r.status)
             for r in task_runs_created],
            transaction=transaction,
        )

        # Bounds для TIME_INTERVAL
        bounds_to_save = [
            TaskRunTimeIntervalExecutionBounds(
                task_run_id=r.id,
                task_id=r.task_id,
                execution_bounds=r.execution_bounds,
            )
            for r in task_runs_created
            if r.type == TaskType.TIME_INTERVAL and r.execution_bounds is not None
        ]
        if bounds_to_save:
            await self._task_run_time_interval_execution_bounds_repo.create_all(
                bounds_to_save, transaction=transaction
            )

        return len(task_runs_created)

    async def _load_last_bounds(
        self,
        tasks: List[Task],
    ) -> Dict[int, TaskRunTimeIntervalExecutionBounds]:
        """
        Для каждой TIME_INTERVAL задачи батчем загружает последнюю запись bounds
        (по right_bound_at desc, limit 1).
        """
        task_ids = [t.id for t in tasks]
        return await self._latest_task_run_time_interval_execution_bounds_provider.provide_latest_bounds_by_task_ids(task_ids)

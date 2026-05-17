from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from more_itertools import batched

from service.domain.schemas.enums import TaskStatus, TaskRunStatus, TaskType
from service.domain.schemas.execution_bounds import ExecutionBounds
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import TaskPK, Task, TaskStatusLog, TaskStatusLogPK
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK, \
    TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK
from service.domain.services.execution_bounds_provider import ExecutionBoundsProvider
from service.domain.services.payload_provider import PayloadProvider
from service.domain.services.task_progress_provider import ActualExecutionBoundsProvider, TimeIntervalExecutionBoundsCutter
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields, FilterFieldsDNF, ConditionOperation
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.ports.outbound.repo.transaction import TransactionFactory


class CreateTaskRunsUCRq(UCRequest):
    pass


class CreateTaskRunsUCRs(UCResponse):
    request: CreateTaskRunsUCRq
    task_runs_created: int


@dataclass(frozen=True)
class TaskRunBuildContext:
    status_updated_at: datetime
    task_group_by_id: Dict[int, TaskGroup]
    payload_by_task: Dict[Task, Payload]
    execution_bounds_by_task: Dict[Task, List[ExecutionBounds]]
    execution_bounds_cutter_by_task_id: Dict[int, TimeIntervalExecutionBoundsCutter]


class TaskRunBuilder(ABC):
    @abstractmethod
    def build(self, task: Task, context: TaskRunBuildContext) -> List[TaskRun]:
        pass

    def _create_task_run(
            self,
            task: Task,
            context: TaskRunBuildContext,
            execution_bounds: Optional[ExecutionBounds],
    ) -> TaskRun:
        task_group = context.task_group_by_id[task.group_id]
        return TaskRun(
            task_id=task.id,
            group_name=task_group.name,
            priority=task.priority,
            type=task.type,
            payload=context.payload_by_task[task],
            execution_bounds=execution_bounds,
            execution_arguments=task.execution_arguments,
            status=TaskRunStatus.WAITING,
            status_updated_at=context.status_updated_at,
        )


class GenericTaskRunBuilder(TaskRunBuilder):
    def build(self, task: Task, context: TaskRunBuildContext) -> List[TaskRun]:
        return [self._create_task_run(task, context, execution_bounds=None)]


class TimeIntervalTaskRunBuilder(TaskRunBuilder):
    def build(self, task: Task, context: TaskRunBuildContext) -> List[TaskRun]:
        execution_bounds_list = context.execution_bounds_by_task.get(task, [])
        if not execution_bounds_list:
            return []

        task_runs: List[TaskRun] = []
        execution_bounds_cutter = context.execution_bounds_cutter_by_task_id.get(task.id)
        for execution_bounds in execution_bounds_list:
            correct_execution_bounds = execution_bounds
            if execution_bounds_cutter:
                correct_execution_bounds = execution_bounds_cutter.cut(execution_bounds)
            if not self._has_positive_interval(correct_execution_bounds):
                continue
            task_runs.append(self._create_task_run(task, context, execution_bounds=correct_execution_bounds))
        return task_runs

    @staticmethod
    def _has_positive_interval(execution_bounds: ExecutionBounds) -> bool:
        left_bound_at = execution_bounds.left_bound_at
        right_bound_at = execution_bounds.right_bound_at
        return left_bound_at is not None and right_bound_at is not None and right_bound_at > left_bound_at


class TaskRunBuilderRegistry:
    def __init__(self, builders_by_task_type: Optional[Dict[TaskType, TaskRunBuilder]] = None):
        self._generic_builder = GenericTaskRunBuilder()
        self._builders_by_task_type = builders_by_task_type or {
            TaskType.TIME_INTERVAL: TimeIntervalTaskRunBuilder(),
        }

    def get(self, task_type: TaskType) -> TaskRunBuilder:
        return self._builders_by_task_type.get(task_type, self._generic_builder)


class CreateTaskRunsUC(UseCase):
    def __init__(self,
                 task_repo: Repo[Task, Task, TaskPK],
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_status_log_repo: Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 task_run_time_interval_execution_bounds_repo: Repo[
                     TaskRunTimeIntervalExecutionBounds,
                     TaskRunTimeIntervalExecutionBounds,
                     TaskRunTimeIntervalExecutionBoundsPK],
                 transaction_factory: TransactionFactory,
                 tasks_to_execute_provider_registry: TaskToExecuteProviderRegistry,
                 execution_bounds_provider: ExecutionBoundsProvider,
                 payload_provider: PayloadProvider,
                 actual_execution_bounds_provider: ActualExecutionBoundsProvider,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 task_run_builder_registry: TaskRunBuilderRegistry = None,
                 tasks_batch_size: int = 5000):
        self._task_repo = task_repo
        self._task_run_repo = task_run_repo
        self._task_status_log_repo = task_status_log_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._task_run_time_interval_execution_bounds_repo = task_run_time_interval_execution_bounds_repo
        self._transaction_factory = transaction_factory
        self._tasks_to_execute_provider_registry = tasks_to_execute_provider_registry
        self._execution_bounds_provider = execution_bounds_provider
        self._payload_provider = payload_provider
        self._actual_execution_bounds_provider = actual_execution_bounds_provider
        self._task_group_repo = task_group_repo
        self._task_run_builder_registry = task_run_builder_registry or TaskRunBuilderRegistry()
        self._tasks_batch_size = tasks_batch_size

    async def apply(self, request: CreateTaskRunsUCRq) -> CreateTaskRunsUCRs:
        tasks_to_create_runs = await self._tasks_to_execute_provider_registry.provide_tasks_to_execute()
        if not tasks_to_create_runs:
            return CreateTaskRunsUCRs(success=True, request=request, task_runs_created=0)

        task_runs_created_count = 0
        async with self._transaction_factory.create() as transaction:
            for tasks_chunk in batched(tasks_to_create_runs, self._tasks_batch_size):
                tasks_chunk = list(tasks_chunk)
                status_updated_at = datetime.now()
                context = await self._build_context(tasks_chunk, status_updated_at)
                task_runs_to_create = self._build_task_runs(tasks_chunk, context)
                task_ids_with_runs = {task_run.task_id for task_run in task_runs_to_create}
                tasks_to_update = [task for task in tasks_chunk if task.id in task_ids_with_runs]
                if not tasks_to_update:
                    continue

                await self._task_repo.update_all({task: UpdateFields.multiple({
                    "status": TaskStatus.EXECUTION,
                    "status_updated_at": status_updated_at})
                    for task in tasks_to_update}, transaction=transaction)
                task_status_logs = self._build_task_status_logs(tasks_to_update, status_updated_at)

                task_runs_created = await self._task_run_repo.create_all(task_runs_to_create, transaction=transaction)
                task_runs_time_interval_execution_bounds = self._build_task_runs_time_interval_execution_bounds(
                    task_runs_created)
                task_run_status_logs = self._build_task_run_status_logs(task_runs_created, status_updated_at)

                await self._task_run_time_interval_execution_bounds_repo.create_all(
                    task_runs_time_interval_execution_bounds, transaction=transaction)
                await self._task_status_log_repo.create_all(task_status_logs, transaction=transaction)
                await self._task_run_status_log_repo.create_all(task_run_status_logs, transaction=transaction)
                task_runs_created_count += len(task_runs_created)
        return CreateTaskRunsUCRs(success=True, request=request, task_runs_created=task_runs_created_count)

    async def _build_context(self, tasks: List[Task], status_updated_at: datetime) -> TaskRunBuildContext:
        task_groups = await self._task_group_repo.filter(
            FilterFieldsDNF.single("id", list({task.group_id for task in tasks}), ConditionOperation.IN)
        )
        payload_by_task = await self._payload_provider.provide(tasks)
        execution_bounds_by_task = await self._execution_bounds_provider.provide_batch(tasks)
        execution_bounds_cutter_by_task_id = await self._actual_execution_bounds_provider.provide(
            [task.id for task in tasks if task.type == TaskType.TIME_INTERVAL]
        )
        return TaskRunBuildContext(
            status_updated_at=status_updated_at,
            task_group_by_id={task_group.id: task_group for task_group in task_groups},
            payload_by_task=payload_by_task,
            execution_bounds_by_task=execution_bounds_by_task,
            execution_bounds_cutter_by_task_id=execution_bounds_cutter_by_task_id,
        )

    def _build_task_runs(self, tasks: List[Task], context: TaskRunBuildContext) -> List[TaskRun]:
        task_runs: List[TaskRun] = []
        for task in tasks:
            task_runs.extend(self._task_run_builder_registry.get(task.type).build(task, context))
        return task_runs

    def _build_task_status_logs(self, tasks: List[Task], status_updated_at: datetime) -> List[TaskStatusLog]:
        return [TaskStatusLog(task_id=task.id,
                              status_updated_at=status_updated_at,
                              status=TaskStatus.EXECUTION)
                for task in tasks]

    def _build_task_run_status_logs(self, task_runs: List[TaskRun], status_updated_at: datetime) -> List[TaskRunStatusLog]:
        return [TaskRunStatusLog(task_run_id=task_run.id,
                                 status_updated_at=status_updated_at,
                                 status=task_run.status)
                for task_run in task_runs]

    def _build_task_runs_time_interval_execution_bounds(
            self,
            task_runs: List[TaskRun],
    ) -> List[TaskRunTimeIntervalExecutionBounds]:
        return [TaskRunTimeIntervalExecutionBounds(
            task_run_id=task_run.id,
            task_id=task_run.task_id,
            execution_bounds=task_run.execution_bounds
        ) for task_run in task_runs
            if task_run.type == TaskType.TIME_INTERVAL and task_run.execution_bounds is not None]

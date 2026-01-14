from datetime import datetime

from service.domain.schemas.enums import TaskStatus, TaskRunStatus
from service.domain.schemas.task import TaskPK, Task, TaskStatusLog, TaskStatusLogPK
from service.domain.schemas.task_run import TaskRunPK, TaskRun, TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.services.payload_provider import PayloadProvider
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.ports.outbound.repo.transaction import TransactionFactory


class CreateTaskRunsUCRq(UCRequest):
    pass


class CreateTaskRunsUCRs(UCResponse):
    request: CreateTaskRunsUCRq
    task_runs_created: int


class CreateTaskRunsUC(UseCase):
    def __init__(self,
                 task_repo: Repo[Task, Task, TaskPK],
                 task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_status_log_repo: Repo[TaskStatusLog, TaskStatusLog, TaskStatusLogPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 transaction_factory: TransactionFactory,
                 tasks_to_execute_provider_registry: TaskToExecuteProviderRegistry,
                 execution_bounds_provider,
                 payload_provider: PayloadProvider, ):
        self._task_repo = task_repo
        self._task_run_repo = task_run_repo
        self._task_status_log_repo = task_status_log_repo
        self._task_run_status_log_repo = task_run_status_log_repo
        self._transaction_factory = transaction_factory
        self._tasks_to_execute_provider_registry = tasks_to_execute_provider_registry
        self._execution_bounds_provider = execution_bounds_provider
        self._payload_provider = payload_provider

    async def apply(self, request: CreateTaskRunsUCRq) -> CreateTaskRunsUCRs:
        async with self._transaction_factory.create() as transaction:
            # Получаем новые задачи
            tasks_to_create_runs = await self._tasks_to_execute_provider_registry.provide_tasks_to_execute()
            # Обновляем статусы задач на "ВЫПОЛНЕНИЕ"
            status_updated_at = datetime.now()
            await self._task_repo.update_all({tasks_to_create_run: UpdateFields.multiple({
                "status": TaskStatus.EXECUTION,
                "status_updated_at": status_updated_at})
                for tasks_to_create_run in tasks_to_create_runs}, transaction=transaction)

            # Для каждой задачи получаем полезную нагрузку
            payload_by_task_pk = await self._payload_provider.provide(tasks_to_create_runs)
            # Для каждой задачи получаем границы выполнения
            execution_bounds_by_task_pk = await self._execution_bounds_provider.provide_batch(tasks_to_create_runs)
            # Создаем массив записей для сохранения в журнал смены статуса задач
            task_status_logs = []
            # Создаем массив запусков задач
            task_runs_to_create = []
            for task_to_create_run in tasks_to_create_runs:
                task_status_logs.append(TaskStatusLog(task_id=task_to_create_run.id,
                                                      status_updated_at=status_updated_at,
                                                      status=TaskStatus.EXECUTION, ))
                payload = payload_by_task_pk[task_to_create_run]
                execution_bounds_list = execution_bounds_by_task_pk[task_to_create_run]
                for execution_bounds in execution_bounds_list:
                    task_runs_to_create.append(TaskRun(task_id=task_to_create_run.id,
                                                       group_name=task_to_create_run.group_name,
                                                       priority=task_to_create_run.priority,
                                                       type=task_to_create_run.type,
                                                       payload=payload,
                                                       execution_bounds=execution_bounds,
                                                       execution_arguments=task_to_create_run.execution_arguments,
                                                       status=TaskRunStatus.WAITING,
                                                       status_updated_at=status_updated_at, ))

            task_runs_created = await self._task_run_repo.create_all(task_runs_to_create, transaction=transaction)
            task_run_status_logs = [TaskRunStatusLog(task_run_id=task_run_created.id,
                                                     status_updated_at=status_updated_at,
                                                     status=task_run_created.status)
                                    for task_run_created in task_runs_created]

            await self._task_status_log_repo.create_all(task_status_logs, transaction=transaction)
            await self._task_run_status_log_repo.create_all(task_run_status_logs, transaction=transaction)
        return CreateTaskRunsUCRs(success=True, request=request, task_runs_created=len(task_runs_created))

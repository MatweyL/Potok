import asyncio
from datetime import datetime

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.base import Base
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.monitoring_algorithm import SAMonitoringAlgorithmRepo, \
    SAPeriodicMonitoringAlgorithmRepo
from service.adapters.outbound.repo.sa.impls.payload import SAPayloadRepo
from service.adapters.outbound.repo.sa.impls.task import SATaskRepo
from service.adapters.outbound.repo.sa.impls.task_run import SATaskRunRepo
from service.adapters.outbound.repo.sa.impls.task_run_status_log import SATaskRunStatusLogRepo
from service.adapters.outbound.repo.sa.impls.task_status_log import SATaskStatusLogRepo
from service.adapters.outbound.repo.sa.impls.time_interval_task_progress import SATimeIntervalTaskProgressRepo
from service.adapters.outbound.repo.sa.transaction import SATransactionFactory
from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider
from service.domain.services.payload_provider import PayloadProvider
from service.domain.use_cases.internal.create_task_runs import CreateTaskRunsUC
from service.ports.common.logs import logger
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.settings import ServiceSettings


async def main():
    settings = ServiceSettings()
    database = Database(settings.database_uri)

    # Создаём таблицы
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    transaction_factory = SATransactionFactory(database)
    monitoring_algorithm = SAMonitoringAlgorithmRepo(database, models.MonitoringAlgorithm)
    periodic_monitoring_algorithm_repo = SAPeriodicMonitoringAlgorithmRepo(database, models.PeriodicMonitoringAlgorithm)
    task_repo = SATaskRepo(database, models.Task)
    task_run_repo = SATaskRunRepo(database, models.TaskRun)
    task_to_execute_provider_registry = TaskToExecuteProviderRegistry([periodic_monitoring_algorithm_repo])
    task_status_log_repo = SATaskStatusLogRepo(database, models.TaskStatusLog)
    task_run_status_log_repo = SATaskRunStatusLogRepo(database, models.TaskRunStatusLog)

    time_interval_task_progress_repo = SATimeIntervalTaskProgressRepo(database, models.TimeIntervalTaskProgress)
    payload_repo = SAPayloadRepo(database, models.Payload)
    execution_bounds_provider = DefaultExecutionBoundsProvider(
        time_interval_progress_repo=time_interval_task_progress_repo,
        default_left_date=datetime(2010, 1, 1),
        default_first_interval_days=31,
    )
    payload_provider = PayloadProvider(payload_repo)

    create_task_runs_uc = CreateTaskRunsUC(task_repo, task_run_repo, task_status_log_repo, task_run_status_log_repo,
                                           transaction_factory, task_to_execute_provider_registry, execution_bounds_provider,
                                           payload_provider)
    try:
        await asyncio.Future()
    except BaseException as e:
        logger.critical(f"Stop service due to error: {e.__class__.__name__}: {e}")


if __name__ == '__main__':
    asyncio.run(main())

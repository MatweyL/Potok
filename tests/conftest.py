import pytest
import pytest_asyncio

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.base import Base
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.monitoring_algorithm import SAPeriodicMonitoringAlgorithmRepo, \
    SAMonitoringAlgorithmRepo, SASingleMonitoringAlgorithmRepo
from service.adapters.outbound.repo.sa.impls.payload import SAPayloadRepo
from service.adapters.outbound.repo.sa.impls.task import SATaskRepo
from service.adapters.outbound.repo.sa.impls.task_run import SATaskRunRepo
from service.adapters.outbound.repo.sa.impls.task_run_status_log import SATaskRunStatusLogRepo
from service.adapters.outbound.repo.sa.impls.task_status_log import SATaskStatusLogRepo
from service.adapters.outbound.repo.sa.impls.time_interval_task_progress import SATimeIntervalTaskProgressRepo
from service.adapters.outbound.repo.sa.transaction import SATransactionFactory
from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider
from service.domain.services.payload_provider import PayloadProvider
from service.domain.services.uniqueness_payload_checker import UniquenessPayloadChecker
from service.domain.use_cases.external.create_tasks import CreateTasksUC
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUC, \
    GetAllMonitoringAlgorithmsUC
from service.domain.use_cases.internal.create_task_runs import CreateTaskRunsUC
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry


@pytest_asyncio.fixture
async def postgres_database():
    """Создаёт подключение к PostgreSQL и очищает данные после тестов"""
    # URL вашей тестовой базы данных
    db_url = "postgresql+asyncpg://postgres:onlyone@localhost:5432/test_db"
    db = Database(db_url)

    # Создаём таблицы
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield db

    # Очистка: удаляем все данные из таблиц
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await db.engine.dispose()


@pytest.fixture
def database(postgres_database):
    return postgres_database


@pytest.fixture
def sa_transaction_factory(database):
    return SATransactionFactory(database)


@pytest.fixture
def sa_payload_repo(database):
    return SAPayloadRepo(database, models.Payload)


@pytest.fixture
def sa_monitoring_algorithm_repo(database):
    return SAMonitoringAlgorithmRepo(database, models.MonitoringAlgorithm)


@pytest.fixture
def sa_periodic_monitoring_algorithm_repo(database):
    return SAPeriodicMonitoringAlgorithmRepo(database, models.PeriodicMonitoringAlgorithm)


@pytest.fixture
def sa_single_monitoring_algorithm_repo(database):
    return SASingleMonitoringAlgorithmRepo(database, models.SingleMonitoringAlgorithm)


@pytest.fixture
def sa_task_repo(database):
    return SATaskRepo(database, models.Task)


@pytest.fixture
def sa_task_run_repo(database):
    return SATaskRunRepo(database, models.TaskRun)


@pytest.fixture
def payload_provider(sa_payload_repo):
    return PayloadProvider(sa_payload_repo)


@pytest.fixture
def task_to_execute_provider_registry(sa_periodic_monitoring_algorithm_repo, sa_single_monitoring_algorithm_repo):
    return TaskToExecuteProviderRegistry([sa_periodic_monitoring_algorithm_repo, sa_single_monitoring_algorithm_repo])


@pytest.fixture
def sa_task_status_log_repo(database):
    return SATaskStatusLogRepo(database, models.TaskStatusLog)


@pytest.fixture
def sa_task_run_status_log_repo(database):
    return SATaskRunStatusLogRepo(database, models.TaskRunStatusLog)


@pytest.fixture
def sa_time_interval_task_progress_repo(database):
    return SATimeIntervalTaskProgressRepo(database, models.TimeIntervalTaskProgress)


@pytest.fixture
def execution_bounds_provider(sa_time_interval_task_progress_repo):
    return DefaultExecutionBoundsProvider(sa_time_interval_task_progress_repo)


@pytest.fixture
def create_task_runs_uc(sa_task_repo, sa_task_run_repo, sa_task_status_log_repo, sa_task_run_status_log_repo,
                        sa_time_interval_task_progress_repo, sa_transaction_factory,
                        task_to_execute_provider_registry, execution_bounds_provider, payload_provider):
    return CreateTaskRunsUC(sa_task_repo, sa_task_run_repo, sa_task_status_log_repo, sa_task_run_status_log_repo,
                            sa_transaction_factory, task_to_execute_provider_registry, execution_bounds_provider,
                            payload_provider, )


@pytest.fixture
def create_monitoring_algorithm_uc(sa_monitoring_algorithm_repo,
                                   sa_periodic_monitoring_algorithm_repo,
                                   sa_single_monitoring_algorithm_repo,
                                   sa_transaction_factory, ) -> CreateMonitoringAlgorithmUC:
    return CreateMonitoringAlgorithmUC(sa_monitoring_algorithm_repo,
                                       sa_periodic_monitoring_algorithm_repo,
                                       sa_single_monitoring_algorithm_repo,
                                       sa_transaction_factory, )


@pytest.fixture
def get_all_monitoring_algorithms_uc(sa_periodic_monitoring_algorithm_repo,
                                     sa_single_monitoring_algorithm_repo) -> GetAllMonitoringAlgorithmsUC:
    return GetAllMonitoringAlgorithmsUC([sa_periodic_monitoring_algorithm_repo, sa_single_monitoring_algorithm_repo, ])


@pytest.fixture
def uniqueness_payload_checker(sa_payload_repo):
    return UniquenessPayloadChecker(sa_payload_repo)


@pytest.fixture
def create_tasks_uc(
        sa_transaction_factory,
        sa_payload_repo,
        sa_task_repo,
        sa_task_status_log_repo,
        uniqueness_payload_checker,
):
    return CreateTasksUC(
        transaction_factory=sa_transaction_factory,
        uniqueness_payload_checker=uniqueness_payload_checker,
        payload_repo=sa_payload_repo,
        task_repo=sa_task_repo,
        task_status_log_repo=sa_task_status_log_repo,
    )

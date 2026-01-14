import pytest
import pytest_asyncio

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.base import Base
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.monitoring_algorithm import SAPeriodicMonitoringAlgorithmRepo, \
    SAMonitoringAlgorithmRepo
from service.adapters.outbound.repo.sa.impls.payload import SAPayloadRepo
from service.adapters.outbound.repo.sa.impls.task import SATaskRepo
from service.domain.services.payload_provider import PayloadProvider


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
def sa_payload_repo(database):
    return SAPayloadRepo(database, models.Payload)


@pytest.fixture
def sa_monitoring_algorithm_repo(database):
    return SAMonitoringAlgorithmRepo(database, models.MonitoringAlgorithm)


@pytest.fixture
def sa_periodic_monitoring_algorithm_repo(database):
    return SAPeriodicMonitoringAlgorithmRepo(database, models.PeriodicMonitoringAlgorithm)

@pytest.fixture
def sa_task_repo(database):
    return SATaskRepo(database, models.Task)
@pytest.fixture
def payload_provider(sa_payload_repo):
    return PayloadProvider(sa_payload_repo)

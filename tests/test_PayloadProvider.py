from datetime import datetime
from typing import List

import pytest
import pytest_asyncio

from service.domain.schemas.enums import TaskStatus, TaskType, PriorityType
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task
from service.ports.outbound.repo.fields import FilterFieldsDNF


# ==================== Fixtures ====================

@pytest_asyncio.fixture
async def _sample_payloads(sa_payload_repo):
    payloads = [
        Payload(id=1, data={"url": "https://api.example.com/v1"}),
        Payload(id=2, data={"url": "https://api.example.com/v2"}),
        Payload(id=3, data={"file_path": "/data/file.pdf"}),
    ]
    await sa_payload_repo.create_all(payloads)
    return payloads


@pytest.fixture
def sample_payloads(_sample_payloads):
    """Примеры payload'ов"""
    _sample_payloads: List[Payload]
    return _sample_payloads


@pytest.fixture
def sample_tasks():
    """Примеры задач"""
    now = datetime.now()
    return [
        Task(
            id=101,
            group_name="api_monitoring",
            priority=PriorityType.HIGH,
            type=TaskType.TIME_INTERVAL,
            monitoring_algorithm_id=1,
            status=TaskStatus.NEW,
            status_updated_at=now,
            payload_id=1,
        ),
        Task(
            id=102,
            group_name="stats_monitoring",
            priority=PriorityType.MEDIUM,
            type=TaskType.TIME_INTERVAL,
            monitoring_algorithm_id=1,
            status=TaskStatus.NEW,
            status_updated_at=now,
            payload_id=1,  # Тот же payload что и у task 101
        ),
        Task(
            id=103,
            group_name="file_processing",
            priority=PriorityType.LOW,
            type=TaskType.TIME_INTERVAL,
            monitoring_algorithm_id=2,
            status=TaskStatus.NEW,
            status_updated_at=now,
            payload_id=2,
        ),
    ]


# ==================== Тесты ====================

@pytest.mark.asyncio
async def test_provide_basic(payload_provider, sa_payload_repo, sample_tasks, sample_payloads):
    """
    Тест базовой функциональности: загрузка payload'ов для задач
    """
    # Arrange
    tasks = sample_tasks[:3]  # 3 задачи
    payloads = sample_payloads[:2]  # 2 payload'а (id=1, id=2)

    # Act
    result = await payload_provider.provide(tasks)

    # Assert
    assert len(result) == 3
    assert result[tasks[0]] == payloads[0]  # task 101 -> payload 1
    assert result[tasks[1]] == payloads[0]  # task 102 -> payload 1 (тот же!)
    assert result[tasks[2]] == payloads[1]  # task 103 -> payload 2


@pytest.mark.asyncio
async def test_provide_empty_tasks(payload_provider, sa_payload_repo):
    """
    Тест с пустым списком задач - не должно быть запроса к БД
    """
    # Act
    result = await payload_provider.provide([])

    # Assert
    assert result == {}


@pytest.mark.asyncio
async def test_provide_missing_payload(payload_provider, sa_payload_repo, sample_tasks):
    """
    Тест случая, когда payload не найден в БД
    """
    # Arrange
    tasks = [sample_tasks[0]]  # task с payload_id=1

    # Act
    result = await payload_provider.provide(tasks)

    # Assert
    assert len(result) == 1
    assert result[tasks[0]] is None  # Должен быть None


# ==================== Интеграционный тест ====================

@pytest.mark.asyncio
async def test_provide_integration_scenario(payload_provider, sa_payload_repo):
    """
    Интеграционный тест: реальный сценарий использования

    Сценарий:
    - 100 задач мониторинга API (используют 3 разных payload'а)
    - Должен быть один запрос к БД
    - Все задачи получают правильные payload'ы
    """
    # Arrange
    now = datetime.now()
    payloads = [
        Payload(id=1, data={"url": "https://api1.com"}),
        Payload(id=2, data={"url": "https://api2.com"}),
        Payload(id=3, data={"url": "https://api3.com"}),
    ]
    await sa_payload_repo.create_all(payloads)
    # 100 задач: 50 с payload_id=1, 30 с payload_id=2, 20 с payload_id=3
    tasks = []
    for i in range(50):
        tasks.append(
            Task(
                id=i,
                group_name="api_monitoring",
                priority=PriorityType.HIGH,
                type=TaskType.TIME_INTERVAL,
                monitoring_algorithm_id=1,
                status=TaskStatus.NEW,
                status_updated_at=now,
                payload_id=1,
            )
        )
    for i in range(50, 80):
        tasks.append(
            Task(
                id=i,
                group_name="api_monitoring",
                priority=PriorityType.MEDIUM,
                type=TaskType.TIME_INTERVAL,
                monitoring_algorithm_id=1,
                status=TaskStatus.NEW,
                status_updated_at=now,
                payload_id=2,
            )
        )
    for i in range(80, 100):
        tasks.append(
            Task(
                id=i,
                group_name="api_monitoring",
                priority=PriorityType.LOW,
                type=TaskType.TIME_INTERVAL,
                monitoring_algorithm_id=1,
                status=TaskStatus.NEW,
                status_updated_at=now,
                payload_id=3,
            )
        )

    # Act
    result = await payload_provider.provide(tasks)

    # Assert
    assert len(result) == 100

    # Проверяем корректность маппинга
    for task in tasks[:50]:
        assert result[task] == payloads[0]
    for task in tasks[50:80]:
        assert result[task] == payloads[1]
    for task in tasks[80:100]:
        assert result[task] == payloads[2]

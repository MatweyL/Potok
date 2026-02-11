from datetime import datetime

import pytest
import pytest_asyncio

from service.domain.schemas.enums import TaskType, TaskStatus, PriorityType, MonitoringAlgorithmType
from service.domain.schemas.monitoring_algorithm import PeriodicMonitoringAlgorithm
from service.domain.schemas.payload import PayloadBody
from service.domain.schemas.task import TaskConfiguration
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def monitoring_algorithm(create_monitoring_algorithm_uc):
    """Create a monitoring algorithm for tasks to reference."""
    algorithm = PeriodicMonitoringAlgorithm(timeout=3600.0, timeout_noize=60.0)
    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)
    return response.created_algorithm


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_tasks_with_new_payloads(create_tasks_uc, monitoring_algorithm):
    """Create tasks with new unique payloads."""
    payloads = [
        PayloadBody(data={"user_id": 1, "action": "login"}),
        PayloadBody(data={"user_id": 2, "action": "logout"}),
        PayloadBody(data={"user_id": 3, "action": "signup"}),
    ]

    task_config = TaskConfiguration(
        group_name="test_group",
        priority=PriorityType.HIGH,
        type=TaskType.TIME_INTERVAL,
        monitoring_algorithm_id=monitoring_algorithm.id,
        execution_arguments={"param1": "value1"},
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert response.tasks is not None
    assert len(response.tasks) == 3

    # Verify task properties
    for task in response.tasks:
        assert task.id is not None
        assert task.group_name == "test_group"
        assert task.priority == PriorityType.HIGH
        assert task.type == TaskType.TIME_INTERVAL
        assert task.monitoring_algorithm_id == monitoring_algorithm.id
        assert task.execution_arguments == {"param1": "value1"}
        assert task.status == TaskStatus.NEW
        assert task.payload_id is not None


@pytest.mark.asyncio
async def test_creates_tasks_with_duplicate_payloads(create_tasks_uc, monitoring_algorithm):
    """When payloads are duplicates, should create tasks for existing payloads."""
    # First, create tasks with some payloads
    payloads_first = [
        PayloadBody(data={"user_id": 10, "action": "login"}),
        PayloadBody(data={"user_id": 11, "action": "logout"}),
    ]

    task_config = TaskConfiguration(
        group_name="first_group",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request1 = CreateTasksUCRq(payloads=payloads_first, task_configuration=task_config)
    response1 = await create_tasks_uc.apply(request1)

    assert response1.success is True
    original_payload_ids = [task.payload_id for task in response1.tasks]

    # Now create tasks with SAME payloads
    payloads_second = [
        PayloadBody(data={"user_id": 10, "action": "login"}),  # Duplicate
        PayloadBody(data={"user_id": 11, "action": "logout"}),  # Duplicate
    ]

    task_config2 = TaskConfiguration(
        group_name="second_group",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request2 = CreateTasksUCRq(payloads=payloads_second, task_configuration=task_config2)
    response2 = await create_tasks_uc.apply(request2)

    assert response2.success is True
    assert len(response2.tasks) == 2

    # Should reuse existing payload IDs
    new_payload_ids = [task.payload_id for task in response2.tasks]
    assert set(new_payload_ids) == set(original_payload_ids)


@pytest.mark.asyncio
async def test_creates_tasks_with_mixed_new_and_existing_payloads(
    create_tasks_uc, monitoring_algorithm
):
    """Mix of new and existing payloads should create appropriate tasks."""
    # Create first batch
    payloads_first = [
        PayloadBody(data={"user_id": 20, "action": "login"}),
    ]

    task_config = TaskConfiguration(
        group_name="batch1",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request1 = CreateTasksUCRq(payloads=payloads_first, task_configuration=task_config)
    response1 = await create_tasks_uc.apply(request1)

    existing_payload_id = response1.tasks[0].payload_id

    # Create second batch with mix
    payloads_second = [
        PayloadBody(data={"user_id": 20, "action": "login"}),  # Existing
        PayloadBody(data={"user_id": 21, "action": "signup"}),  # New
    ]

    request2 = CreateTasksUCRq(payloads=payloads_second, task_configuration=task_config)
    response2 = await create_tasks_uc.apply(request2)

    assert response2.success is True
    assert len(response2.tasks) == 2

    # One should have existing payload_id, one should have new
    payload_ids = [task.payload_id for task in response2.tasks]
    assert existing_payload_id in payload_ids


@pytest.mark.asyncio
async def test_creates_single_task(create_tasks_uc, monitoring_algorithm):
    """Create a single task."""
    payloads = [PayloadBody(data={"single": "task"})]

    task_config = TaskConfiguration(
        group_name="single_task_group",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert len(response.tasks) == 1
    assert response.tasks[0].group_name == "single_task_group"


@pytest.mark.asyncio
async def test_task_configuration_with_defaults(create_tasks_uc, monitoring_algorithm):
    """Task configuration with default values."""
    payloads = [PayloadBody(data={"test": "data"})]

    task_config = TaskConfiguration(
        group_name="default_config",
        monitoring_algorithm_id=monitoring_algorithm.id,
        # priority and type use defaults
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert response.tasks[0].priority == PriorityType.MEDIUM  # Default
    assert response.tasks[0].type == TaskType.UNDEFINED  # Default


@pytest.mark.asyncio
async def test_task_configuration_with_custom_priority(create_tasks_uc, monitoring_algorithm):
    """Create tasks with custom priority."""
    payloads = [PayloadBody(data={"priority": "test"})]

    task_config = TaskConfiguration(
        group_name="priority_test",
        priority=PriorityType.LOW,
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert response.tasks[0].priority == PriorityType.LOW


@pytest.mark.asyncio
async def test_task_configuration_with_execution_arguments(
    create_tasks_uc, monitoring_algorithm
):
    """Create tasks with execution arguments."""
    payloads = [PayloadBody(data={"exec": "args"})]

    exec_args = {
        "timeout": 3600,
        "retries": 3,
        "endpoint": "https://api.example.com",
    }

    task_config = TaskConfiguration(
        group_name="exec_args_test",
        monitoring_algorithm_id=monitoring_algorithm.id,
        execution_arguments=exec_args,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert response.tasks[0].execution_arguments == exec_args


@pytest.mark.asyncio
async def test_task_configuration_without_execution_arguments(
    create_tasks_uc, monitoring_algorithm
):
    """Create tasks without execution arguments."""
    payloads = [PayloadBody(data={"no": "exec_args"})]

    task_config = TaskConfiguration(
        group_name="no_exec_args",
        monitoring_algorithm_id=monitoring_algorithm.id,
        execution_arguments=None,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert response.tasks[0].execution_arguments is None


@pytest.mark.asyncio
async def test_payload_checksum_auto_generated(create_tasks_uc, monitoring_algorithm):
    """Payload checksum is automatically generated if not provided."""
    payloads = [
        PayloadBody(data={"auto": "checksum"}),  # No checksum provided
    ]

    task_config = TaskConfiguration(
        group_name="checksum_test",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True

    # Verify payload was created with checksum
    from service.domain.schemas.payload import PayloadPK

    payload_id = response.tasks[0].payload_id
    retrieved_payload = await create_tasks_uc._payload_repo.get(PayloadPK(id=payload_id))

    assert retrieved_payload.checksum is not None
    assert len(str(retrieved_payload.checksum.hex)) == 32  # MD5 hash length


@pytest.mark.asyncio
async def test_all_tasks_have_same_configuration(create_tasks_uc, monitoring_algorithm):
    """All tasks created from same request share the same configuration."""
    payloads = [
        PayloadBody(data={"id": i}) for i in range(5)
    ]

    task_config = TaskConfiguration(
        group_name="shared_config",
        priority=PriorityType.HIGH,
        type=TaskType.TIME_INTERVAL,
        monitoring_algorithm_id=monitoring_algorithm.id,
        execution_arguments={"shared": "value"},
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert len(response.tasks) == 5

    # All should have identical config
    for task in response.tasks:
        assert task.group_name == "shared_config"
        assert task.priority == PriorityType.HIGH
        assert task.type == TaskType.TIME_INTERVAL
        assert task.monitoring_algorithm_id == monitoring_algorithm.id
        assert task.execution_arguments == {"shared": "value"}


@pytest.mark.asyncio
async def test_response_contains_request(create_tasks_uc, monitoring_algorithm):
    """Response contains the original request."""
    payloads = [PayloadBody(data={"test": "request"})]

    task_config = TaskConfiguration(
        group_name="request_test",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.request is request


@pytest.mark.asyncio
async def test_created_tasks_persist_in_database(
    create_tasks_uc, monitoring_algorithm, sa_task_repo
):
    """Verify tasks are actually persisted in database."""
    payloads = [PayloadBody(data={"persist": "test"})]

    task_config = TaskConfiguration(
        group_name="persistence_test",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    task_id = response.tasks[0].id

    # Retrieve from database directly
    from service.domain.schemas.task import TaskPK

    retrieved_task = await sa_task_repo.get(TaskPK(id=task_id))

    assert retrieved_task is not None
    assert retrieved_task.id == task_id
    assert retrieved_task.group_name == "persistence_test"


@pytest.mark.asyncio
async def test_large_batch_of_tasks(create_tasks_uc, monitoring_algorithm):
    """Create a large batch of tasks (100+)."""
    payloads = [PayloadBody(data={"batch_id": i}) for i in range(150)]

    task_config = TaskConfiguration(
        group_name="large_batch",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert len(response.tasks) == 150


@pytest.mark.asyncio
async def test_empty_payload_data(create_tasks_uc, monitoring_algorithm):
    """Create tasks with empty payload data."""
    payloads = [PayloadBody(data={})]  # Empty dict

    task_config = TaskConfiguration(
        group_name="empty_payload",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert len(response.tasks) == 1


@pytest.mark.asyncio
async def test_null_payload_data(create_tasks_uc, monitoring_algorithm):
    """Create tasks with None payload data."""
    payloads = [PayloadBody(data=None)]

    task_config = TaskConfiguration(
        group_name="null_payload",
        monitoring_algorithm_id=monitoring_algorithm.id,
    )

    request = CreateTasksUCRq(payloads=payloads, task_configuration=task_config)
    response = await create_tasks_uc.apply(request)

    assert response.success is True
    assert len(response.tasks) == 1

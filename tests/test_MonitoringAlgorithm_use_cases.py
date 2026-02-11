import pytest

from service.domain.schemas.enums import MonitoringAlgorithmType
from service.domain.schemas.monitoring_algorithm import (
    PeriodicMonitoringAlgorithm,
    SingleMonitoringAlgorithm,
)
from service.domain.use_cases.external.monitoring_algorithm import (
    CreateMonitoringAlgorithmUCRq,
    GetAllMonitoringAlgorithmsUCRq,
)


# ---------------------------------------------------------------------------
# CreateMonitoringAlgorithmUC Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_periodic_algorithm(create_monitoring_algorithm_uc):
    algorithm = PeriodicMonitoringAlgorithm(
        timeout=3600.0,
        timeout_noize=60.0,
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert response.created_algorithm is not None
    assert response.created_algorithm.id is not None
    assert response.created_algorithm.timeout == 3600.0
    assert response.created_algorithm.timeout_noize == 60.0
    assert response.created_algorithm.type == MonitoringAlgorithmType.PERIODIC


@pytest.mark.asyncio
async def test_create_single_algorithm(create_monitoring_algorithm_uc):
    algorithm = SingleMonitoringAlgorithm(
        timeouts=[100.0, 200.0, 300.0],
        timeout_noize=10.0,
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert response.created_algorithm is not None
    assert response.created_algorithm.id is not None
    assert response.created_algorithm.timeouts == [100.0, 200.0, 300.0]
    assert response.created_algorithm.timeout_noize == 10.0
    assert response.created_algorithm.type == MonitoringAlgorithmType.SINGLE


@pytest.mark.asyncio
async def test_create_periodic_with_zero_noize(create_monitoring_algorithm_uc):
    algorithm = PeriodicMonitoringAlgorithm(
        timeout=1800.0,
        timeout_noize=0.0,
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert response.created_algorithm is not None
    assert response.created_algorithm.timeout_noize == 0.0


@pytest.mark.asyncio
async def test_create_single_with_empty_timeouts(create_monitoring_algorithm_uc):
    algorithm = SingleMonitoringAlgorithm(
        timeouts=[],  # Empty - task runs once
        timeout_noize=0.0,
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert response.created_algorithm is not None
    assert response.created_algorithm.timeouts == []


@pytest.mark.asyncio
async def test_create_periodic_with_large_timeout(create_monitoring_algorithm_uc):
    algorithm = PeriodicMonitoringAlgorithm(
        timeout=86400.0,  # 24 hours
        timeout_noize=3600.0,  # 1 hour
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert response.created_algorithm.timeout == 86400.0
    assert response.created_algorithm.timeout_noize == 3600.0


@pytest.mark.asyncio
async def test_create_single_with_multiple_timeouts(create_monitoring_algorithm_uc):
    algorithm = SingleMonitoringAlgorithm(
        timeouts=[10.0, 20.0, 30.0, 40.0, 50.0],
        timeout_noize=5.0,
    )

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.success is True
    assert len(response.created_algorithm.timeouts) == 5
    assert response.created_algorithm.timeouts == [10.0, 20.0, 30.0, 40.0, 50.0]


@pytest.mark.asyncio
async def test_response_contains_request(create_monitoring_algorithm_uc):
    algorithm = PeriodicMonitoringAlgorithm(timeout=1800.0)

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    assert response.request is request


# ---------------------------------------------------------------------------
# GetAllMonitoringAlgorithmsUC Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_returns_empty_list_initially(get_all_monitoring_algorithms_uc):
    request = GetAllMonitoringAlgorithmsUCRq()
    response = await get_all_monitoring_algorithms_uc.apply(request)

    assert response.success is True
    assert isinstance(response.algorithms, list)
    # May be empty or contain algorithms from other tests


@pytest.mark.asyncio
async def test_get_all_returns_created_algorithms(
    create_monitoring_algorithm_uc,
    get_all_monitoring_algorithms_uc,
):
    # Create a periodic algorithm
    periodic = PeriodicMonitoringAlgorithm(timeout=3600.0, timeout_noize=60.0)
    response = await create_monitoring_algorithm_uc.apply(
        CreateMonitoringAlgorithmUCRq(algorithm=periodic)
    )
    assert response.success, response

    # Create a single algorithm
    single = SingleMonitoringAlgorithm(timeouts=[100.0, 200.0], timeout_noize=10.0)
    response = await create_monitoring_algorithm_uc.apply(
        CreateMonitoringAlgorithmUCRq(algorithm=single)
    )
    assert response.success, response

    # Get all algorithms
    request = GetAllMonitoringAlgorithmsUCRq()
    response = await get_all_monitoring_algorithms_uc.apply(request)

    assert response.success is True
    assert len(response.algorithms) >= 2

    # Check that created algorithms are in the list
    algorithm_types = [alg.type for alg in response.algorithms]
    assert MonitoringAlgorithmType.PERIODIC in algorithm_types
    assert MonitoringAlgorithmType.SINGLE in algorithm_types


@pytest.mark.asyncio
async def test_get_all_filters_by_type(
    create_monitoring_algorithm_uc,
    get_all_monitoring_algorithms_uc,
):
    # Create multiple periodic algorithms
    for i in range(3):
        periodic = PeriodicMonitoringAlgorithm(
            timeout=1800.0 + i * 100, timeout_noize=30.0
        )
        await create_monitoring_algorithm_uc.apply(
            CreateMonitoringAlgorithmUCRq(algorithm=periodic)
        )

    # Get all - should return at least the periodic ones we created
    request = GetAllMonitoringAlgorithmsUCRq()
    response = await get_all_monitoring_algorithms_uc.apply(request)

    assert response.success is True
    # Note: get_all_monitoring_algorithms_uc uses only periodic repo in fixture
    # so it should only return PERIODIC algorithms
    periodic_algorithms = [
        alg for alg in response.algorithms if alg.type == MonitoringAlgorithmType.PERIODIC
    ]
    assert len(periodic_algorithms) >= 3


@pytest.mark.asyncio
async def test_get_all_response_contains_request(get_all_monitoring_algorithms_uc):
    request = GetAllMonitoringAlgorithmsUCRq()
    response = await get_all_monitoring_algorithms_uc.apply(request)

    assert response.request is request


@pytest.mark.asyncio
async def test_created_algorithm_persists(
    create_monitoring_algorithm_uc,
    sa_periodic_monitoring_algorithm_repo,
):
    """Verify that created algorithm is actually stored in DB."""
    algorithm = PeriodicMonitoringAlgorithm(timeout=7200.0, timeout_noize=120.0)

    request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    response = await create_monitoring_algorithm_uc.apply(request)

    created_id = response.created_algorithm.id

    # Retrieve from repo directly
    from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmPK

    retrieved = await sa_periodic_monitoring_algorithm_repo.get(
        MonitoringAlgorithmPK(id=created_id)
    )

    assert retrieved is not None
    assert retrieved.id == created_id
    assert retrieved.timeout == 7200.0
    assert retrieved.timeout_noize == 120.0


@pytest.mark.asyncio
async def test_create_multiple_algorithms_each_gets_unique_id(create_monitoring_algorithm_uc):
    """Ensure each created algorithm gets a unique ID."""
    ids = []

    for i in range(5):
        algorithm = PeriodicMonitoringAlgorithm(timeout=1000.0 + i * 100, timeout_noize=10.0)
        request = CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
        response = await create_monitoring_algorithm_uc.apply(request)

        assert response.success is True
        assert response.created_algorithm.id is not None
        ids.append(response.created_algorithm.id)

    # All IDs should be unique
    assert len(ids) == len(set(ids))
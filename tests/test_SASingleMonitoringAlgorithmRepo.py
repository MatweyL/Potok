from datetime import datetime
from typing import List

import pytest

from service.domain.schemas.enums import TaskStatus, TaskType, MonitoringAlgorithmType
from service.domain.schemas.monitoring_algorithm import SingleMonitoringAlgorithm, MonitoringAlgorithm
from service.domain.schemas.payload import Payload
from service.domain.schemas.task import Task


# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def _make_task(sa_task_repo, sa_payload_repo):

    async def _inner(
        task_id: int,
        loaded_at: datetime,
        status: TaskStatus,
        status_updated_at: datetime,
    ) -> Task:
        payload = await sa_payload_repo.create(Payload(data={'username': 'test'}))
        task = Task(
            id=task_id,
            type=TaskType.TIME_INTERVAL,
            status=status,
            status_updated_at=status_updated_at,
            payload_id=payload.id,
            monitoring_algorithm_id=1,
            group_name="test",
            loaded_at=loaded_at,
    )
        return await sa_task_repo.create(task)
    return _inner


@pytest.fixture
def _make_algorithm(sa_monitoring_algorithm_repo, sa_single_monitoring_algorithm_repo):
    async def _inner(timeouts: List[float], timeout_noize: float = 0.0) -> SingleMonitoringAlgorithm:
        monitoring_algorithm = await sa_monitoring_algorithm_repo.create(MonitoringAlgorithm(id=1,
                                                                                             type=MonitoringAlgorithmType.SINGLE))
        single_monitoring_algorithm = SingleMonitoringAlgorithm(
            id=monitoring_algorithm.id,
            timeouts=timeouts,
            timeout_noize=timeout_noize,
        )
        
        return single_monitoring_algorithm
    return _inner


# ---------------------------------------------------------------------------
# Test _calculate_execution_intervals
# ---------------------------------------------------------------------------


class TestCalculateExecutionIntervals:
    """Тесты для вычисления интервалов выполнения."""

    @pytest.mark.asyncio
    async def test_empty_timeouts_single_interval_to_infinity(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        algorithm = await _make_algorithm(timeouts=[])
        task = await _make_task(
            task_id=1,
            loaded_at=datetime(2024, 1, 1, 0, 0, 0),
            status=TaskStatus.NEW,
            status_updated_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        intervals = repo._calculate_execution_intervals(task, algorithm)

        assert len(intervals) == 1
        assert intervals[0][0] == datetime(2024, 1, 1, 0, 0, 0)
        assert intervals[0][1] == datetime.max

    @pytest.mark.asyncio
    async def test_single_timeout_creates_two_intervals(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        algorithm = await _make_algorithm(timeouts=[3600.0])  # 1 hour
        task = await _make_task(
            task_id=1,
            loaded_at=datetime(2024, 1, 1, 0, 0, 0),
            status=TaskStatus.NEW,
            status_updated_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        intervals = repo._calculate_execution_intervals(task, algorithm)

        # Interval 0: [loaded_at, loaded_at + 3600s]
        # Interval 1: [loaded_at + 3600s, infinity]
        assert len(intervals) == 2
        assert intervals[0][0] == datetime(2024, 1, 1, 0, 0, 0)
        assert intervals[0][1] == datetime(2024, 1, 1, 1, 0, 0)
        assert intervals[1][0] == datetime(2024, 1, 1, 1, 0, 0)
        assert intervals[1][1] == datetime.max

    @pytest.mark.asyncio
    async def test_multiple_timeouts_creates_n_plus_one_intervals(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        algorithm = await _make_algorithm(timeouts=[100.0, 200.0, 300.0])
        task = await _make_task(
            task_id=1,
            loaded_at=datetime(2024, 1, 1, 0, 0, 0),
            status=TaskStatus.NEW,
            status_updated_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        intervals = repo._calculate_execution_intervals(task, algorithm)

        # 3 timeouts → 4 intervals
        assert len(intervals) == 4

        # Interval 0: [0, 100s]
        assert intervals[0][0] == datetime(2024, 1, 1, 0, 0, 0)
        assert intervals[0][1] == datetime(2024, 1, 1, 0, 1, 40)

        # Interval 1: [100s, 300s]
        assert intervals[1][0] == datetime(2024, 1, 1, 0, 1, 40)
        assert intervals[1][1] == datetime(2024, 1, 1, 0, 5, 0)

        # Interval 2: [300s, 600s]
        assert intervals[2][0] == datetime(2024, 1, 1, 0, 5, 0)
        assert intervals[2][1] == datetime(2024, 1, 1, 0, 10, 0)

        # Interval 3: [600s, infinity]
        assert intervals[3][0] == datetime(2024, 1, 1, 0, 10, 0)
        assert intervals[3][1] == datetime.max


# ---------------------------------------------------------------------------
# Test _find_current_interval
# ---------------------------------------------------------------------------


class TestFindCurrentInterval:
    """Тесты для поиска текущего интервала."""

    @pytest.mark.asyncio
    async def test_finds_first_interval(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)),
            (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 0, 30, 0)

        current = repo._find_current_interval(intervals, now)

        assert current == (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0))

    @pytest.mark.asyncio
    async def test_finds_second_interval(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)),
            (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 1, 30, 0)

        current = repo._find_current_interval(intervals, now)

        assert current == (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0))

    @pytest.mark.asyncio
    async def test_returns_none_if_before_all_intervals(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 0, 30, 0)

        current = repo._find_current_interval(intervals, now)

        assert current is None

    @pytest.mark.asyncio
    async def test_returns_none_if_after_all_intervals(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 2, 0, 0)

        current = repo._find_current_interval(intervals, now)

        assert current is None

    @pytest.mark.asyncio
    async def test_left_bound_inclusive(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 1, 0, 0)  # Exactly at left_bound

        current = repo._find_current_interval(intervals, now)

        assert current == (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0))

    @pytest.mark.asyncio
    async def test_right_bound_exclusive(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        intervals = [
            (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)),
            (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)),
        ]
        now = datetime(2024, 1, 1, 1, 0, 0)  # Exactly at right_bound of first interval

        current = repo._find_current_interval(intervals, now)

        # Should find second interval
        assert current == (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0))


# ---------------------------------------------------------------------------
# Test _is_task_ready_to_execute
# ---------------------------------------------------------------------------


class TestIsTaskReadyToExecute:
    """Тесты для проверки готовности задачи к выполнению."""

    @pytest.mark.asyncio
    async def test_new_task_in_current_interval_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.NEW,
            status_updated_at=loaded_at,
        )
        now = datetime(2024, 1, 1, 0, 30, 0)  # In first interval

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is True

    @pytest.mark.asyncio
    async def test_succeed_task_status_updated_before_left_bound_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])  # Interval 1 starts at 01:00
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.SUCCEED,
            status_updated_at=datetime(2024, 1, 1, 0, 30, 0),  # Before second interval
        )
        now = datetime(2024, 1, 1, 1, 30, 0)  # In second interval

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is True

    @pytest.mark.asyncio
    async def test_succeed_task_status_updated_after_left_bound_not_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.SUCCEED,
            status_updated_at=datetime(2024, 1, 1, 1, 30, 0),  # After left_bound
        )
        now = datetime(2024, 1, 1, 1, 45, 0)  # In second interval (starts at 01:00)

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is False

    @pytest.mark.asyncio
    async def test_execution_status_not_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.EXECUTION,
            status_updated_at=loaded_at,
        )
        now = datetime(2024, 1, 1, 0, 30, 0)

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is False

    @pytest.mark.asyncio
    async def test_finished_status_not_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.FINISHED,
            status_updated_at=loaded_at,
        )
        now = datetime(2024, 1, 1, 0, 30, 0)

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is False

    @pytest.mark.asyncio
    async def test_no_current_interval_not_ready(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[3600.0])
        task = await _make_task(
            task_id=1,
            loaded_at=loaded_at,
            status=TaskStatus.NEW,
            status_updated_at=loaded_at,
        )
        now = datetime(2023, 12, 31, 23, 0, 0)  # Before loaded_at

        ready = repo._is_task_ready_to_execute(task, algorithm, now)

        assert ready is False


# ---------------------------------------------------------------------------
# Integration-style scenarios
# ---------------------------------------------------------------------------


class TestTaskExecutionScenarios:
    """Интеграционные сценарии для полной логики."""

    @pytest.mark.asyncio
    async def test_task_executes_in_all_intervals(self, _make_task, _make_algorithm,
                                                  sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[100.0, 200.0, 300.0])

        # Interval 0: NEW task ready immediately
        task = await _make_task(1, loaded_at, TaskStatus.NEW, loaded_at)  # id=1
        now = datetime(2024, 1, 1, 0, 0, 30)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is True

        # Interval 1
        task = await _make_task(2, loaded_at, TaskStatus.SUCCEED, datetime(2024, 1, 1, 0, 0, 50))  # id=2
        now = datetime(2024, 1, 1, 0, 2, 0)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is True

        # Interval 2
        task = await _make_task(3, loaded_at, TaskStatus.SUCCEED, datetime(2024, 1, 1, 0, 2, 10))  # id=3
        now = datetime(2024, 1, 1, 0, 6, 0)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is True

        # Interval 3
        task = await _make_task(4, loaded_at, TaskStatus.SUCCEED, datetime(2024, 1, 1, 0, 6, 30))  # id=4
        now = datetime(2024, 1, 1, 0, 11, 0)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is True

    @pytest.mark.asyncio
    async def test_empty_timeouts_executes_once(self, _make_task, _make_algorithm, sa_single_monitoring_algorithm_repo):
        repo = sa_single_monitoring_algorithm_repo
        loaded_at = datetime(2024, 1, 1, 0, 0, 0)
        algorithm = await _make_algorithm(timeouts=[])

        # First execution: NEW task ready
        task = await _make_task(1, loaded_at, TaskStatus.NEW, loaded_at)  # id=1
        now = datetime(2024, 1, 1, 0, 0, 30)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is True

        # After execution, status → SUCCEED
        task = await _make_task(2, loaded_at, TaskStatus.SUCCEED, datetime(2024, 1, 1, 0, 1, 0))  # id=2
        now = datetime(2024, 1, 1, 0, 2, 0)
        assert repo._is_task_ready_to_execute(task, algorithm, now) is False

from datetime import datetime

import pytest

from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task_progress import TimeIntervalTaskProgress
from service.domain.services.task_progress_provider import TimeInterval, TimeIntervalExecutionBoundsCutter


# ---------------------------------------------------------------------------
# TimeInterval tests
# ---------------------------------------------------------------------------


class TestTimeInterval:
    """Tests for TimeInterval helper class."""

    def test_has_date_within_interval(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_date(datetime(2024, 1, 1, 6, 0, 0)) is True

    def test_has_date_at_left_boundary(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        # Left boundary is inclusive
        assert interval.has_date(datetime(2024, 1, 1, 0, 0, 0)) is True

    def test_has_date_at_right_boundary(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        # Right boundary is inclusive
        assert interval.has_date(datetime(2024, 1, 1, 12, 0, 0)) is True

    def test_has_date_before_interval(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_date(datetime(2023, 12, 31, 23, 0, 0)) is False

    def test_has_date_after_interval(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_date(datetime(2024, 1, 1, 13, 0, 0)) is False

    def test_is_date_greater(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.is_date_greater(datetime(2024, 1, 1, 13, 0, 0)) is True
        assert interval.is_date_greater(datetime(2024, 1, 1, 12, 0, 0)) is False

    def test_is_date_lower(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.is_date_lower(datetime(2023, 12, 31, 23, 0, 0)) is True
        assert interval.is_date_lower(datetime(2024, 1, 1, 0, 0, 0)) is False

    def test_has_interval_fully_inside(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_interval(
            datetime(2024, 1, 1, 2, 0, 0),
            datetime(2024, 1, 1, 10, 0, 0)
        ) is True

    def test_has_interval_exact_match(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_interval(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 12, 0, 0)
        ) is True

    def test_has_interval_partially_outside(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert interval.has_interval(
            datetime(2024, 1, 1, 2, 0, 0),
            datetime(2024, 1, 1, 13, 0, 0)  # Right outside
        ) is False

    def test_is_interval_lower(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
        )

        # Interval completely before
        assert interval.is_interval_lower(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 6, 0, 0)
        ) is True

    def test_is_interval_greater(self):
        interval = TimeInterval(
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            right_bound_at=datetime(2024, 1, 1, 6, 0, 0),
        )

        # Interval completely after
        assert interval.is_interval_greater(
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 18, 0, 0)
        ) is True


# ---------------------------------------------------------------------------
# TimeIntervalExecutionBoundsCutter - create_intervals tests
# ---------------------------------------------------------------------------


class TestCreateIntervals:
    """Tests for interval creation logic."""

    def test_empty_progress_list_returns_empty_intervals(self):
        cutter = TimeIntervalExecutionBoundsCutter([])
        assert cutter.intervals == []

    def test_single_progress_creates_single_interval(self):
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )

        cutter = TimeIntervalExecutionBoundsCutter([progress])

        assert len(cutter.intervals) == 1
        assert cutter.intervals[0].left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert cutter.intervals[0].right_bound_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_non_overlapping_progresses_create_separate_intervals(self):
        """Two completely separate progress records → two intervals."""
        progress1 = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 6, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )
        progress2 = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )

        cutter = TimeIntervalExecutionBoundsCutter([progress1, progress2])

        assert len(cutter.intervals) == 2
        # Sorted by right_bound_at descending
        assert cutter.intervals[0].right_bound_at == datetime(2024, 1, 1, 18, 0, 0)
        assert cutter.intervals[1].right_bound_at == datetime(2024, 1, 1, 6, 0, 0)

    def test_fully_contained_progress_is_skipped(self):
        """If a progress is fully inside another, it's skipped."""
        progress_outer = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            collected_data_amount=20,
            saved_data_amount=20,
        )
        progress_inner = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 6, 0, 0),
            collected_data_amount=5,
            saved_data_amount=5,
        )

        cutter = TimeIntervalExecutionBoundsCutter([progress_outer, progress_inner])

        # Should only have the outer interval
        assert len(cutter.intervals) == 1
        assert cutter.intervals[0].left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert cutter.intervals[0].right_bound_at == datetime(2024, 1, 1, 18, 0, 0)

    def test_overlapping_progress_extends_left_bound(self):
        """If right_bound_at is inside but left_bound_at is outside, extend left_bound."""
        progress1 = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )
        progress2 = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 15, 0, 0),  # Inside progress1
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),  # Before progress1
            collected_data_amount=20,
            saved_data_amount=20,
        )

        cutter = TimeIntervalExecutionBoundsCutter([progress1, progress2])

        # Should merge into one interval with extended left bound
        assert len(cutter.intervals) == 1
        assert cutter.intervals[0].left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert cutter.intervals[0].right_bound_at == datetime(2024, 1, 1, 18, 0, 0)

    def test_multiple_overlapping_progresses(self):
        """Multiple overlapping records should merge correctly."""
        progresses = [
            TimeIntervalTaskProgress(
                task_id=1,
                right_bound_at=datetime(2024, 1, 1, 20, 0, 0),
                left_bound_at=datetime(2024, 1, 1, 15, 0, 0),
                collected_data_amount=5,
                saved_data_amount=5,
            ),
            TimeIntervalTaskProgress(
                task_id=1,
                right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
                left_bound_at=datetime(2024, 1, 1, 10, 0, 0),
                collected_data_amount=8,
                saved_data_amount=8,
            ),
            TimeIntervalTaskProgress(
                task_id=1,
                right_bound_at=datetime(2024, 1, 1, 16, 0, 0),
                left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
                collected_data_amount=16,
                saved_data_amount=16,
            ),
        ]

        cutter = TimeIntervalExecutionBoundsCutter(progresses)

        # All should merge into one
        assert len(cutter.intervals) == 1
        assert cutter.intervals[0].left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert cutter.intervals[0].right_bound_at == datetime(2024, 1, 1, 20, 0, 0)


# ---------------------------------------------------------------------------
# TimeIntervalExecutionBoundsCutter - cut tests
# ---------------------------------------------------------------------------


class TestCut:
    """Tests for cutting execution bounds."""

    def test_cut_with_no_intervals_returns_unchanged(self):
        cutter = TimeIntervalExecutionBoundsCutter([])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        assert result.left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert result.right_bound_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_cut_fully_executed_interval_returns_zero_width(self):
        """If interval is fully executed, return left == right."""
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )
        cutter = TimeIntervalExecutionBoundsCutter([progress])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        # Fully executed → left == right
        assert result.left_bound_at == result.right_bound_at
        assert result.left_bound_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_cut_interval_before_executed_returns_unchanged(self):
        """Execution bounds before any executed interval → unchanged."""
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )
        cutter = TimeIntervalExecutionBoundsCutter([progress])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 6, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        # Completely before executed interval
        assert result.left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert result.right_bound_at == datetime(2024, 1, 1, 6, 0, 0)

    def test_cut_interval_partially_executed(self):
        """Right bound overlaps with executed interval → cut at executed left_bound."""
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 6, 0, 0),
            collected_data_amount=10,
            saved_data_amount=10,
        )
        cutter = TimeIntervalExecutionBoundsCutter([progress])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        # Cut at the executed interval's left bound
        assert result.left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert result.right_bound_at == datetime(2024, 1, 1, 6, 0, 0)

    def test_cut_with_multiple_executed_intervals(self):
        """Multiple executed intervals → cut at the earliest one that overlaps."""
        progresses = [
            TimeIntervalTaskProgress(
                task_id=1,
                right_bound_at=datetime(2024, 1, 1, 6, 0, 0),
                left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
                collected_data_amount=6,
                saved_data_amount=6,
            ),
            TimeIntervalTaskProgress(
                task_id=1,
                right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
                left_bound_at=datetime(2024, 1, 1, 12, 0, 0),
                collected_data_amount=6,
                saved_data_amount=6,
            ),
        ]
        cutter = TimeIntervalExecutionBoundsCutter(progresses)

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 20, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        # Should cut at the later executed interval (12-18)
        assert result.left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert result.right_bound_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_cut_interval_exact_match_with_executed(self):
        """Bounds exactly match executed interval → zero width."""
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 6, 0, 0),
            collected_data_amount=6,
            saved_data_amount=6,
        )
        cutter = TimeIntervalExecutionBoundsCutter([progress])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 6, 0, 0),
        )

        result = cutter.cut(bounds)

        assert result.left_bound_at == result.right_bound_at
        assert result.left_bound_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_cut_preserves_left_bound(self):
        """Left bound is never modified, only right bound is cut."""
        progress = TimeIntervalTaskProgress(
            task_id=1,
            right_bound_at=datetime(2024, 1, 1, 12, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 8, 0, 0),
            collected_data_amount=4,
            saved_data_amount=4,
        )
        cutter = TimeIntervalExecutionBoundsCutter([progress])

        bounds = TimeIntervalBounds(
            right_bound_at=datetime(2024, 1, 1, 18, 0, 0),
            left_bound_at=datetime(2024, 1, 1, 0, 0, 0),
        )

        result = cutter.cut(bounds)

        # Left bound preserved
        assert result.left_bound_at == datetime(2024, 1, 1, 0, 0, 0)
        assert result.right_bound_at == datetime(2024, 1, 1, 8, 0, 0)
        
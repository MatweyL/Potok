import enum

from imitation_modelling.broker import Broker
from imitation_modelling.repo import TaskRunStatusRepo, TaskRunMetricProvider
from imitation_modelling.schemas import TaskBatchProviderType, SystemTime
from imitation_modelling.task_batch_provider import TaskBatchProvider


class MovingPIDState(str, enum.Enum):
    COLD_START = "COLD_START"
    RANGE_RETENTION = "RANGE_RETENTION"
    ADJUSTMENT = "ADJUSTMENT"


class MovingPIDProvider(TaskBatchProvider):
    type: TaskBatchProviderType.MOVING_PID

    def __init__(self, broker: Broker, task_run_status_repo: TaskRunStatusRepo,
                 task_run_metric_provider: TaskRunMetricProvider, system_time: SystemTime,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 1.5,
                 range_retention_iterations: int = 3,
                 adjustment_grow_multiplier: float = 1.2,
                 adjustment_shrink_multiplier: float = 0.6,
                 throughput_growth_threshold: float = 0.05,
                 # FIX [suggestion]: configurable hard ceiling to prevent runaway growth
                 batch_size_hard_max: float = 0.0):
        super().__init__(broker, task_run_status_repo, task_run_metric_provider, system_time)
        self._state = MovingPIDState.COLD_START
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier
        self._range_retention_iterations = range_retention_iterations
        self._adjustment_grow_multiplier = adjustment_grow_multiplier
        self._adjustment_shrink_multiplier = adjustment_shrink_multiplier
        self._throughput_growth_threshold = throughput_growth_threshold

        self._batch_size = 1.0
        self._batch_size_min = 1.0
        self._batch_size_max = 2.0  # Ensure max > min from the start
        # FIX [suggestion]: hard ceiling; 0.0 means "unset, derive after cold start"
        self._batch_size_hard_max = float(batch_size_hard_max)

        self._ema_throughput = 0.0
        self._prev_ema_throughput = 0.0
        self._last_completed_count = 0
        self._calls_count = 0
        self._range_retention_calls = 0

        # FIX [warning]: track consecutive panics to escalate response under sustained overload
        self._consecutive_panic_count = 0

        # FIX [warning]: flag for throughput degradation — avoids double-incrementing the counter
        self._degradation_detected = False

    # ---------------------------------------------------------------------------
    # FIX [critical]: centralized range invariant enforcement.
    # Call after every mutation of _batch_size_min or _batch_size_max.
    # ---------------------------------------------------------------------------
    def _clamp_range(self) -> None:
        """Enforce batch_size_max > batch_size_min at all times."""
        self._batch_size_min = max(1.0, self._batch_size_min)
        self._batch_size_max = max(self._batch_size_min + 1.0, self._batch_size_max)

    def _apply_panic_mode(self) -> None:
        """Emergency brake: reset batch size toward real throughput capacity."""
        self._consecutive_panic_count += 1

        # FIX [warning]: escalate hard floor after repeated panics to escape sustained overload loops
        if self._consecutive_panic_count >= 3:
            # Aggressive floor: halve the current max instead of anchoring to EMA
            self._batch_size_max = max(2.0, self._batch_size_max * 0.5)
            self._batch_size_min = max(1.0, self._batch_size_min * 0.5)
        else:
            self._batch_size_max = max(2.0, int(self._ema_throughput * 1.1))
            self._batch_size_min = max(1.0, self._ema_throughput * 0.5)

        self._clamp_range()  # FIX [critical]: enforce invariant after every mutation
        self._batch_size = max(self._batch_size_min, self._ema_throughput * 0.8)
        self._range_retention_calls = 0
        self._degradation_detected = False

        # FIX [suggestion]: log panic event for production debugging
        self._log_event("panic", extra={
            "consecutive_panic_count": self._consecutive_panic_count,
            "new_batch_size_min": self._batch_size_min,
            "new_batch_size_max": self._batch_size_max,
        })

    def _log_event(self, event: str, extra: dict = None) -> None:
        """
        FIX [suggestion]: structured logging on every state transition and panic.
        Replace with your actual logger (e.g. logging.getLogger(__name__).info(...)).
        """
        import logging
        logger = logging.getLogger(__name__)
        payload = {
            "event": event,
            "state": self._state.name if hasattr(self._state, "name") else str(self._state),
            "calls_count": self._calls_count,
            "batch_size": self._batch_size,
            "batch_size_min": self._batch_size_min,
            "batch_size_max": self._batch_size_max,
            "ema_throughput": round(self._ema_throughput, 3),
            "prev_ema_throughput": round(self._prev_ema_throughput, 3),
        }
        if extra:
            payload.update(extra)
        logger.debug("MovingPIDUltra %s", payload)

    def calculate_batch_size(self) -> int:
        metrics = self._task_run_metric_provider
        completed = metrics.get_completed_count()

        # 1. Metrics and noise filtering
        throughput = completed - self._last_completed_count
        self._ema_throughput = 0.2 * throughput + 0.8 * self._ema_throughput

        overload = metrics.get_temp_error_count_total() + metrics.get_interrupted_count_total()
        in_flight = metrics.get_execution_count_total() + metrics.get_queued_count_total()

        self._last_completed_count = completed
        self._calls_count += 1

        # 2. State machine
        if self._state == MovingPIDState.COLD_START:
            if self._calls_count == 1:
                initial = float(self._cold_start_batch_size or max(1, metrics.get_total_count() // 10))
                self._batch_size = initial
                # FIX [suggestion]: derive hard max from cold-start size if not explicitly set
                if self._batch_size_hard_max <= 0.0:
                    self._batch_size_hard_max = initial * 10.0

            else:
                # FIX [warning]: skip in_flight check for the first few calls while EMA warms up
                in_flight_overloaded = (
                    self._calls_count >= 3
                    and in_flight > self._ema_throughput * 2.5
                )
                if overload > 0 or in_flight_overloaded:
                    self._batch_size_min = max(1.0, self._ema_throughput * 0.5)
                    self._batch_size_max = max(self._batch_size_min + 1.0, self._batch_size)
                    self._clamp_range()  # FIX [critical]
                    self._state = MovingPIDState.RANGE_RETENTION
                    self._apply_panic_mode()
                    self._log_event("cold_start→range_retention(panic)")
                else:
                    self._batch_size *= self._cold_start_growth_multiplier

        elif self._state == MovingPIDState.RANGE_RETENTION:
            self._range_retention_calls += 1

            if overload > 0:
                self._apply_panic_mode()
                # State stays RANGE_RETENTION; panic resets the counter
            else:
                # FIX [warning]: use a degradation flag instead of double-incrementing the counter.
                # Double-incrementing could skip RANGE_RETENTION entirely on a single bad tick.
                if self._ema_throughput < self._prev_ema_throughput * 0.7:
                    self._degradation_detected = True

                # Smooth drift toward range center
                target = (self._batch_size_min + self._batch_size_max) / 2.0
                self._batch_size = 0.3 * target + 0.7 * self._batch_size

            # FIX [warning]: exit one iteration early only if degradation was detected
            effective_threshold = (
                max(1, self._range_retention_iterations - 1)
                if self._degradation_detected
                else self._range_retention_iterations
            )
            if self._range_retention_calls >= effective_threshold:
                self._state = MovingPIDState.ADJUSTMENT
                self._log_event("range_retention→adjustment")

        elif self._state == MovingPIDState.ADJUSTMENT:
            # FIX [critical]: guard against prev_ema_throughput == 0 on the very first ADJUSTMENT.
            # Skip the cycle and seed the baseline so growth ratio is meaningful next time.
            if self._prev_ema_throughput < 1e-3:
                self._prev_ema_throughput = self._ema_throughput
                self._range_retention_calls = 0
                self._degradation_detected = False
                self._state = MovingPIDState.RANGE_RETENTION
                self._log_event("adjustment→range_retention(ema_seed)")
                # Fall through to validation and return
            else:
                growth = self._ema_throughput / (self._prev_ema_throughput + 1e-6)

                if overload > 0 or growth < (1.0 - self._throughput_growth_threshold):
                    self._batch_size_max = self._batch_size_max * self._adjustment_shrink_multiplier
                    self._batch_size_min = self._batch_size_min * self._adjustment_shrink_multiplier
                    self._clamp_range()  # FIX [critical]: enforce invariant after shrink
                    self._log_event("adjustment:shrink", extra={"growth_ratio": round(growth, 4)})
                elif growth > (1.0 + self._throughput_growth_threshold):
                    self._batch_size_min *= self._adjustment_grow_multiplier
                    self._batch_size_max *= self._adjustment_grow_multiplier
                    self._clamp_range()  # FIX [critical]: enforce invariant after grow
                    self._log_event("adjustment:grow", extra={"growth_ratio": round(growth, 4)})
                else:
                    # No significant change — reset consecutive panic counter on stable cycle
                    self._consecutive_panic_count = 0

                self._prev_ema_throughput = self._ema_throughput
                self._range_retention_calls = 0
                self._degradation_detected = False
                self._state = MovingPIDState.RANGE_RETENTION
                self._batch_size = (self._batch_size_min + self._batch_size_max) / 2.0

        # 3. Validation and hard ceiling
        self._batch_size = max(1.0, self._batch_size)
        # FIX [suggestion]: hard ceiling prevents runaway growth under sustained throughput
        if self._batch_size_hard_max > 0.0:
            self._batch_size = min(self._batch_size, self._batch_size_hard_max)

        return int(self._batch_size)
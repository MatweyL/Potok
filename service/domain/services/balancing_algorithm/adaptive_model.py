import enum
from dataclasses import dataclass
from typing import Dict, List

from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_run_metrics import TaskRunGroupedMetrics
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider


class AdaptiveModelState(str, enum.Enum):
    COLD_START = "COLD_START"
    EXPLOITATION = "EXPLOITATION"
    EXPLORATION = "EXPLORATION"


@dataclass
class AdaptiveModelGroupState:
    state: AdaptiveModelState = AdaptiveModelState.COLD_START

    # Model reference points: batch sizes (b_*) and their observed throughput (t_*).
    b_low: float = 0.0
    b_peak: float = 0.0
    b_high: float = 0.0
    t_low: float = 0.0
    t_peak: float = 0.0
    t_high: float = 0.0

    batch_size: float = 0.0
    ema_throughput: float = 0.0
    calls_count: int = 0
    exploitation_calls: int = 0
    exploration_direction: int = 1
    exploration_steps_done: int = 0

    integral_error: float = 0.0
    last_pid_error: float = 0.0
    last_in_flight: int = 0


class AdaptiveModelBalancingAlgorithm(BalancingAlgorithm):
    def __init__(self,
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
                 task_run_metrics_provider: TaskRunMetricsProvider,
                 batch_size_min: int = 1,
                 batch_size_max: int = 1_000_000,
                 period_s: int = 600,
                 cold_start_batch_size: int = 0,
                 cold_start_growth_multiplier: float = 2.5,
                 model_alpha_low: float = 0.111,
                 model_alpha_peak: float = 0.346,
                 model_alpha_high: float = 0.455,
                 throughput_ema_alpha: float = 0.254,
                 exploration_interval: int = 10,
                 exploration_step_fraction: float = 0.067,
                 min_model_width: int = 8,
                 overload_rate_threshold: float = 0.28,
                 kp: float = 0.4,
                 ki: float = 0.05,
                 kd: float = 0.5,
                 ):
        super().__init__(task_group_repo)
        self._task_run_metrics_provider = task_run_metrics_provider
        self._period_s = period_s
        self._batch_size_min = batch_size_min
        self._batch_size_max = batch_size_max
        self._cold_start_batch_size = cold_start_batch_size
        self._cold_start_growth_multiplier = cold_start_growth_multiplier
        self._model_alpha_low = model_alpha_low
        self._model_alpha_peak = model_alpha_peak
        self._model_alpha_high = model_alpha_high
        self._throughput_ema_alpha = throughput_ema_alpha
        self._exploration_interval = exploration_interval
        self._exploration_step_fraction = exploration_step_fraction
        self._min_model_width = min_model_width
        self._overload_rate_threshold = overload_rate_threshold
        self._kp = kp
        self._ki = ki
        self._kd = kd
        self._state_by_group: Dict[str, AdaptiveModelGroupState] = {}

    async def calculate_batch_size_by_group(self, group_names: List[str]) -> Dict[str, int]:
        task_run_metrics = await self._task_run_metrics_provider.provide_by_period(self._period_s, group_names)

        batch_size_by_group = {}
        for group_name in group_names:
            state = self._state_by_group.setdefault(group_name, AdaptiveModelGroupState())
            metrics = task_run_metrics.grouped_metrics_by_name.get(group_name)
            batch_size_by_group[group_name] = self._calculate_batch_size(state, metrics)

        logger.info(f"Adaptive model batch sizes: {batch_size_by_group}")
        return batch_size_by_group

    def _calculate_batch_size(self,
                              state: AdaptiveModelGroupState,
                              metrics: TaskRunGroupedMetrics | None,
                              ) -> int:
        completed_count = metrics.completed if metrics else 0
        temp_error_count = metrics.temp_error if metrics else 0
        interrupted_count = metrics.interrupted if metrics else 0
        execution_count = metrics.execution if metrics else 0
        queued_count = metrics.queued if metrics else 0
        waiting_count = metrics.waiting if metrics else 0

        self._update_throughput_ema(state, completed_count)

        overload = temp_error_count + interrupted_count
        in_flight = execution_count + queued_count
        in_tail = waiting_count < max(state.batch_size, 1)
        is_overloaded = self._is_overloaded(overload, in_flight)

        state.calls_count += 1
        target_batch = state.batch_size

        if state.state == AdaptiveModelState.COLD_START:
            target_batch = self._calculate_cold_start_target(state, is_overloaded, in_flight)
        elif state.state == AdaptiveModelState.EXPLOITATION:
            target_batch = self._calculate_exploitation_target(state, is_overloaded, in_tail)
        elif state.state == AdaptiveModelState.EXPLORATION:
            target_batch = self._calculate_exploration_target(state, is_overloaded, in_tail)

        if state.state != AdaptiveModelState.COLD_START:
            state.batch_size += self._apply_pid(state, target_batch, in_flight)

        state.batch_size = self._clip_batch_size(state.batch_size, state)
        return int(state.batch_size)

    def _calculate_cold_start_target(self,
                                     state: AdaptiveModelGroupState,
                                     is_overloaded: bool,
                                     in_flight: int,
                                     ) -> float:
        if state.calls_count == 1:
            state.batch_size = float(self._cold_start_batch_size or self._batch_size_min)
            return state.batch_size

        if is_overloaded or in_flight > state.ema_throughput * 3:
            state.b_high = max(state.batch_size, float(self._batch_size_min + self._min_model_width))
            state.b_low = max(float(self._batch_size_min), state.batch_size / 4)
            state.b_peak = (state.b_low + state.b_high) / 2
            state.t_low = state.ema_throughput * 0.5
            state.t_peak = state.ema_throughput
            state.t_high = state.ema_throughput * 0.5
            state.state = AdaptiveModelState.EXPLOITATION
            self._enforce_model_consistency(state)
            return state.b_peak

        state.batch_size *= self._cold_start_growth_multiplier
        return state.batch_size

    def _calculate_exploitation_target(self,
                                       state: AdaptiveModelGroupState,
                                       is_overloaded: bool,
                                       in_tail: bool,
                                       ) -> float:
        if in_tail:
            return state.batch_size

        self._assign_observation_to_zone(state, state.batch_size, state.ema_throughput)
        if is_overloaded:
            state.b_high = max(state.b_peak + self._min_model_width, state.batch_size * 0.85)
            self._enforce_model_consistency(state)
            state.integral_error *= 0.5

        state.exploitation_calls += 1
        if state.exploitation_calls >= self._exploration_interval:
            state.exploration_direction = 1 if (
                state.exploitation_calls // self._exploration_interval
            ) % 2 == 1 else -1
            state.state = AdaptiveModelState.EXPLORATION
            return self._predict_best_batch(state) + (
                state.exploration_direction * self._exploration_step_size(state)
            )

        return self._predict_best_batch(state)

    def _calculate_exploration_target(self,
                                      state: AdaptiveModelGroupState,
                                      is_overloaded: bool,
                                      in_tail: bool,
                                      ) -> float:
        if in_tail:
            return state.batch_size

        self._assign_observation_to_zone(state, state.batch_size, state.ema_throughput)
        if is_overloaded:
            state.b_high = max(state.b_peak + self._min_model_width, state.batch_size * 0.85)
            state.state = AdaptiveModelState.EXPLOITATION
            state.exploration_steps_done = 0
            self._enforce_model_consistency(state)
            return self._predict_best_batch(state)

        state.exploration_steps_done += 1
        if state.exploration_steps_done < 2:
            return state.batch_size + state.exploration_direction * self._exploration_step_size(state)

        state.state = AdaptiveModelState.EXPLOITATION
        state.exploration_steps_done = 0
        return self._predict_best_batch(state)

    def _update_throughput_ema(self, state: AdaptiveModelGroupState, completed_count: int) -> None:
        state.ema_throughput = (
            self._throughput_ema_alpha * completed_count
            + (1 - self._throughput_ema_alpha) * state.ema_throughput
        )

    def _is_overloaded(self, overload: int, in_flight: int) -> bool:
        if in_flight == 0:
            return False
        return (overload / in_flight) > self._overload_rate_threshold

    def _apply_pid(self,
                   state: AdaptiveModelGroupState,
                   target_batch: float,
                   current_in_flight: int,
                   ) -> float:
        error = target_batch - state.batch_size
        p_term = self._kp * error

        state.integral_error = max(-20.0, min(20.0, state.integral_error + error))
        i_term = self._ki * state.integral_error

        in_flight_delta = current_in_flight - state.last_in_flight
        d_term = self._kd * in_flight_delta

        state.last_pid_error = error
        state.last_in_flight = current_in_flight

        return p_term + i_term - d_term

    def _assign_observation_to_zone(self,
                                    state: AdaptiveModelGroupState,
                                    batch: float,
                                    throughput: float,
                                    ) -> None:
        mid_low = (state.b_low + state.b_peak) / 2
        mid_high = (state.b_peak + state.b_high) / 2

        if batch <= mid_low:
            state.b_low = (1 - self._model_alpha_low) * state.b_low + self._model_alpha_low * batch
            state.t_low = (1 - self._model_alpha_low) * state.t_low + self._model_alpha_low * throughput
        elif batch >= mid_high:
            state.b_high = (1 - self._model_alpha_high) * state.b_high + self._model_alpha_high * batch
            state.t_high = (1 - self._model_alpha_high) * state.t_high + self._model_alpha_high * throughput
        else:
            state.b_peak = (1 - self._model_alpha_peak) * state.b_peak + self._model_alpha_peak * batch
            state.t_peak = (1 - self._model_alpha_peak) * state.t_peak + self._model_alpha_peak * throughput

        self._enforce_model_consistency(state)

    def _enforce_model_consistency(self, state: AdaptiveModelGroupState) -> None:
        half = self._min_model_width / 2
        state.b_low = min(state.b_low, state.b_peak - half)
        state.b_high = max(state.b_high, state.b_peak + half)
        state.b_low = max(float(self._batch_size_min), state.b_low)
        state.b_peak = max(state.b_low + half, state.b_peak)
        state.b_high = min(float(self._batch_size_max), max(state.b_high, state.b_peak + half))

    def _predict_best_batch(self, state: AdaptiveModelGroupState) -> float:
        if state.t_peak >= state.t_low and state.t_peak >= state.t_high:
            return state.b_peak
        if state.t_low > state.t_peak:
            return (state.b_peak + state.b_high) / 2
        return state.b_peak

    def _exploration_step_size(self, state: AdaptiveModelGroupState) -> float:
        return max(1.0, self._exploration_step_fraction * (state.b_high - state.b_low))

    def _clip_batch_size(self, batch_size: float, state: AdaptiveModelGroupState) -> float:
        batch_size = max(float(self._batch_size_min), min(batch_size, float(self._batch_size_max)))
        if state.b_high > 0:
            batch_size = min(batch_size, state.b_high)
        return batch_size

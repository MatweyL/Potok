import json
from pathlib import Path
from time import perf_counter

from imitation_modelling.repo import TaskRunMetricProvider
from imitation_modelling.schemas import SystemTime, SimulationParams


class MetricCollector:
    def __init__(self, system_time: SystemTime, metric_provider: TaskRunMetricProvider,
                 params: SimulationParams, ):
        self.system_time = system_time
        self.metric_provider = metric_provider
        self.params = params
        self.metrics_history = []

        self._start_counter = None
        self._is_stopped = False
        self._duration = 0

    def stop(self):
        self._is_stopped = True
        if self._start_counter is None:
            self._start_counter = perf_counter()
        self._duration = perf_counter() - self._start_counter

    @property
    def duration(self) -> float:
        if self._is_stopped:
            return self._duration
        if self._start_counter is None:
            self._start_counter = perf_counter()
        self._duration = perf_counter() - self._start_counter
        return self._duration

    def collect(self):
        metrics = {
            'time': int((self.system_time.current - self.system_time.start).total_seconds()),
            'executionCount': self.metric_provider.get_execution_count_total(),
            'queuedCount': self.metric_provider.get_queued_count_total(),
            'waitingCount': self.metric_provider.get_waiting_count_total(),
            'queuedAvgDuration': round(self.metric_provider.get_queued_average_duration(), 2),
            'executionAvgDuration': round(self.metric_provider.get_execution_average_duration(), 2),
            'returnFrequency': round(self.metric_provider.get_return_frequency(), 4),
            'succeedFrequency': round(self.metric_provider.get_succeed_frequency(), 4),
            'completed': self.metric_provider.get_completed_count(),
            'total': self.metric_provider.get_total_count()
        }
        self.metrics_history.append(metrics)
        if self._start_counter is None:
            self._start_counter = perf_counter()

    def save(self, ):
        duration = self.duration
        print(f"saving; duration: {duration} s")

        d = {'history': self.metrics_history,
             'params': self.params.model_dump(),
             'duration': duration,
             'run_name': self.params.run_name}
        file_dir = Path("simulation_results")
        file_dir.mkdir(parents=True, exist_ok=True)
        file = file_dir.joinpath(self.params.run_name + '.json')
        file.write_text(json.dumps(d, indent=2, default=str))

import logging

from imitation_modelling.schemas import SimulationParams, TaskBatchProviderType
from imitation_modelling.system_runner import build_system_runner
from service.ports.common.logs import set_log_level


def main():
    set_log_level(logging.INFO)
    params = SimulationParams(task_batch_provider_type=TaskBatchProviderType.PID,
                              task_batch_provider_params=dict(delta=1, beta=0.95,
                                                              base_batch_size=100,
                                                              batch_size_min=10,
                                                              batch_size_max=10000, ),
                              handlers_amount=10,
                              handler_max_tasks=10,
                              tasks_part_from_all_for_high_load=0.8,
                              time_step_seconds=50,
                              random_timeout_generator_left=1,
                              random_timeout_generator_right=52,
                              tasks_amount=16000,
                              max_run_seconds=180,
                              )
    system_runner = build_system_runner(params)
    system_runner.run()


if __name__ == '__main__':
    main()

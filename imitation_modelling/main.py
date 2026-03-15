import logging

from imitation_modelling.schemas import SimulationParams, TaskBatchProviderType
from imitation_modelling.system_runner import build_system_runner
from service.ports.common.logs import set_log_level



def main():
    set_log_level(logging.INFO)
    params = SimulationParams(task_batch_provider_type=TaskBatchProviderType.CONSTANT_SIZE,
                              handlers_amount=10,
                              handler_max_tasks=10,
                              tasks_part_from_all_for_high_load=0.6,
                              temp_error_probability_at_high_load=0.9,
                              time_step_seconds=15,
                              random_timeout_generator_left=20,
                              random_timeout_generator_right=25,
                              tasks_amount=1000,
                              max_run_seconds=180,
                              )
    system_runner = build_system_runner(params)
    system_runner.run()


if __name__ == '__main__':
    main()

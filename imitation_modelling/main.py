import json
import logging
import random
from pathlib import Path

from imitation_modelling.schemas import SimulationParams, SystemParams, TaskBatchProviderParams, TaskBatchProviderType
from imitation_modelling.system_runner import build_system_runner
from service.ports.common.logs import set_log_level

set_log_level(logging.INFO)


def main():
    simulation_configs_path = Path(__file__).parent.joinpath('simulation_configs.json')
    simulation_configs = [SystemParams(**sc) for sc in json.loads(simulation_configs_path.read_text('utf-8'))]
    simulation_config_by_name = {sc.config_name: sc for sc in simulation_configs}
    algorithm_configs_path = simulation_configs_path.parent.joinpath('algo_params.json')
    algorithm_configs = [TaskBatchProviderParams(**ac) for ac in json.loads(algorithm_configs_path.read_text('utf-8'))]

    simulation_params = [SimulationParams(task_batch_provider_params=ac,
                                          system_params=simulation_config_by_name[ac.system_config_name]) for ac in
                         algorithm_configs]
    filtered_simulation_params = ([sp for sp in simulation_params if sp.task_batch_provider_params.system_config_name == 'cfg_087__medium__strict_load__scale_up'
                        ])
    for index, sp in enumerate(filtered_simulation_params):
        sp.system_params.metric_provider_period = 300
        system_runner = build_system_runner(sp)
        system_runner.run()


if __name__ == '__main__':
    main()

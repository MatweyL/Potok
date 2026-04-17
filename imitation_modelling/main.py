import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from imitation_modelling.schemas import SimulationParams, SystemParams, TaskBatchProviderParams, TaskBatchProviderType
from imitation_modelling.system_runner import build_system_runner
from service.ports.common.logs import set_log_level, logger

set_log_level(logging.WARNING)

simulation_results_path = Path("simulation_results")
if simulation_results_path.exists():
    already_executed_runs = {file.name.replace('.json', '') for file in list(simulation_results_path.iterdir())}
else:
    already_executed_runs = {}

# ====================== НАСТРОЙКИ ======================
MAX_WORKERS = max(1, os.cpu_count())

print(f"Запускаем симуляции на {MAX_WORKERS} ядрах (всего ядер: {os.cpu_count()})")


# ====================== ФУНКЦИЯ ДЛЯ ОДНОЙ СИМУЛЯЦИИ ======================
def run_single_simulation(sp):
    """Эта функция будет выполняться в отдельном процессе"""
    try:
        sp.system_params.metric_provider_period = 300
        sp.system_params.max_run_seconds = 30

        system_runner = build_system_runner(sp)
        system_runner.run()

        return sp  # или любой результат, который тебе нужен

    except Exception as e:
        logger.error(f"Ошибка при выполнении симуляции {getattr(sp, 'name', 'unknown')}: {e}")
        return None


# ====================== ОСНОВНОЙ ПАРАЛЛЕЛЬНЫЙ ЗАПУСК ======================
def run_all_simulations(filtered_simulation_params):
    results = []
    total = len(filtered_simulation_params)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Подаём все задачи на выполнение
        future_to_index = {
            executor.submit(run_single_simulation, sp): index
            for index, sp in enumerate(filtered_simulation_params)
        }

        # Обрабатываем результаты по мере завершения
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                result = future.result()
                results.append(result)

                # Прогресс-бар (выводим каждые 5–10%, чтобы не спамить)
                if (index + 1) % max(1, total // 20) == 0 or (index + 1) == total:
                    percent = (index + 1) / total * 100
                    logger.warning(f"Готово: {percent:.2f}%  ({index + 1}/{total})")

            except Exception as exc:
                logger.error(f"Симуляция {index} завершилась с ошибкой: {exc}")

    return results


def main():
    simulation_configs_path = Path(__file__).parent.joinpath('simulation_configs.json')
    simulation_configs = [SystemParams(**sc) for sc in json.loads(simulation_configs_path.read_text('utf-8'))]
    simulation_config_by_name = {sc.config_name: sc for sc in simulation_configs}
    algorithm_configs_path = simulation_configs_path.parent.joinpath('algo_params.json')
    algorithm_configs = [TaskBatchProviderParams(**ac) for ac in json.loads(algorithm_configs_path.read_text('utf-8'))]

    simulation_params = [SimulationParams(task_batch_provider_params=ac,
                                          system_params=simulation_config_by_name[ac.system_config_name]) for ac in
                         algorithm_configs]
    filtered_simulation_params = ([
        sp for sp in simulation_params
        if sp.run_name not in already_executed_runs
           and sp.task_batch_provider_params.type in (
               TaskBatchProviderType.AIMD,
               TaskBatchProviderType.CONSTANT_SIZE,
               TaskBatchProviderType.GRADIENT_ASCENT,
               TaskBatchProviderType.ADAPTIVE_MODEL,
           )
    ]
    )
    logger.warning(f"total configuration: {len(simulation_params)}; filtered: {len(filtered_simulation_params)}")

    run_all_simulations(filtered_simulation_params)


if __name__ == '__main__':
    main()

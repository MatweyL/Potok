import json
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, Any

from imitation_modelling.system_runner import SimulationParams, build_system_runner


def run_simulation_from_config(config_path: Path) -> Dict[str, Any]:
    """
    Запускает симуляцию из файла конфигурации
    Эта функция будет выполняться в отдельном процессе
    """
    try:
        print(f"🔄 Обработка: {config_path.name}")

        # Загружаем параметры
        params = SimulationParams.model_validate_json(config_path.read_text('utf-8'))

        # Создаем и запускаем симуляцию
        system_runner = build_system_runner(params)
        system_runner.run()

        # Собираем результаты
        result = {
            'config_file': config_path.name,
            'success': True,
            'tasks_completed': system_runner.metric_provider.get_completed_count(),
            'tasks_total': system_runner.metric_provider.get_total_count(),
            'completion_rate': system_runner.metric_provider.get_completed_count() / system_runner.metric_provider.get_total_count(),
        }

        print(f"✅ Завершено: {config_path.name} ({result['tasks_completed']}/{result['tasks_total']})")
        return result

    except Exception as e:
        print(f"❌ Ошибка в {config_path.name}: {e}")
        return {
            'config_file': config_path.name,
            'success': False,
            'error': str(e)
        }


def main():
    # Собираем пути к конфигам (вместо создания runner'ов сразу)
    config_files = []
    for item in Path("simulation_configs").iterdir():
        if item.name.endswith('.json'):
            config_files.append(item)

    print(f"📦 Найдено {len(config_files)} конфигураций")

    # Параллельный запуск на 8 ядрах
    num_processes = 8
    print(f"🚀 Запуск на {num_processes} ядрах процессора")
    print(f"⏱️  Начало обработки...\n")

    with Pool(processes=num_processes) as pool:
        results = pool.map(run_simulation_from_config, config_files)

    # Анализ результатов
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]

    print(f"\n{'=' * 60}")
    print(f"📊 Итоговые результаты:")
    print(f"{'=' * 60}")
    print(f"✅ Успешно завершено: {len(successful)}/{len(results)}")
    print(f"❌ Ошибок: {len(failed)}")

    if successful:
        avg_completion = sum(r['completion_rate'] for r in successful) / len(successful)
        print(f"📈 Средний процент завершения задач: {avg_completion:.1%}")

    # Сохраняем результаты
    output_file = "simulation_results.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Результаты сохранены в '{output_file}'")

    # Если были ошибки, выводим их
    if failed:
        print(f"\n⚠️  Конфигурации с ошибками:")
        for r in failed:
            print(f"  - {r['config_file']}: {r.get('error', 'Unknown error')}")


if __name__ == '__main__':
    main()

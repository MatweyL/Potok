import json
import random
from pathlib import Path
from typing import List, Dict, Any


class ConfigGenerator:
    """Генератор конфигурационных файлов для симуляции"""

    def __init__(self, output_dir: str = "configs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def generate_random_config(self, config_id: int) -> Dict[str, Any]:
        """Генерирует случайную конфигурацию с реалистичными параметрами"""

        # Базовые параметры с вариациями
        handlers_amount = random.choice([5, 10, 15, 20, 25, 30])
        handler_max_tasks = random.choice([1, 2, 3, 5])

        # Таймауты
        execution_confirm_timeout = random.choice([60, 120, 180, 240, 300, 360, 420, 480])
        interrupted_timeout = execution_confirm_timeout + random.randint(10, 100)

        # Нагрузка и ошибки
        tasks_part_from_all_for_high_load = round(random.uniform(0.6, 0.95), 2)
        temp_error_probability_at_high_load = round(random.uniform(0.001, 0.2), 3)

        # Время выполнения задач
        min_execution = random.randint(10, 30)
        max_execution = min_execution + random.randint(5, 30)

        # Количество задач
        tasks_amount = random.choice([100, 500, 1000, 2000, 5000, 10000])

        # Параметры отправки
        run_timeout = random.choice([10, 20, 30, 40, 50, 60])
        batch_size = random.choice([10, 25, 50, 100, 200, 500])

        # Метрики и время
        metric_provider_period = random.choice([60, 90, 120, 150, 180, 240, 300])
        time_step_seconds = random.choice([1, 5, 10, 15, 20, 25, 30])

        return {
            "handlers_amount": handlers_amount,
            "handler_max_tasks": handler_max_tasks,
            "execution_confirm_timeout": execution_confirm_timeout,
            "tasks_part_from_all_for_high_load": tasks_part_from_all_for_high_load,
            "temp_error_probability_at_high_load": temp_error_probability_at_high_load,
            "random_timeout_generator_left": min_execution,
            "random_timeout_generator_right": max_execution,
            "tasks_amount": tasks_amount,
            "interrupted_timeout": interrupted_timeout,
            "run_timeout": run_timeout,
            "batch_size": batch_size,
            "metric_provider_period": metric_provider_period,
            "time_step_seconds": time_step_seconds
        }

    def generate_grid_configs(self) -> List[Dict[str, Any]]:
        """Генерирует конфигурации на основе сетки параметров"""
        configs = []

        # Определяем варианты для каждого параметра
        handlers_variants = [5, 10, 15, 20]
        handler_max_tasks_variants = [1, 2, 3]
        tasks_amounts = [500, 1000, 2000, 5000]
        error_probabilities = [0.01, 0.05, 0.1, 0.15]

        config_id = 0
        for handlers in handlers_variants:
            for max_tasks in handler_max_tasks_variants:
                for tasks in tasks_amounts:
                    for error_prob in error_probabilities:
                        if config_id >= 100:
                            return configs

                        config = {
                            "handlers_amount": handlers,
                            "handler_max_tasks": max_tasks,
                            "execution_confirm_timeout": 300,
                            "tasks_part_from_all_for_high_load": 0.9,
                            "temp_error_probability_at_high_load": error_prob,
                            "random_timeout_generator_left": 25,
                            "random_timeout_generator_right": 35,
                            "tasks_amount": tasks,
                            "interrupted_timeout": 310,
                            "run_timeout": 30,
                            "batch_size": 100,
                            "metric_provider_period": 150,
                            "time_step_seconds": 1
                        }
                        configs.append(config)
                        config_id += 1

        return configs

    def generate_mixed_configs(self, count: int = 100) -> List[Dict[str, Any]]:
        """Генерирует смесь: 50% случайных + 50% сеточных конфигураций"""
        configs = []

        # Половина - случайные
        for i in range(count // 2):
            configs.append(self.generate_random_config(i))

        # Половина - сеточные (для покрытия важных комбинаций)
        grid_configs = self.generate_grid_configs()
        configs.extend(grid_configs[:count - len(configs)])

        return configs[:count]

    def save_configs(self, configs: List[Dict[str, Any]], prefix: str = "config"):
        """Сохраняет конфигурации в отдельные JSON файлы"""
        for i, config in enumerate(configs):
            filename = self.output_dir / f"{prefix}_{i:03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

        print(f"✅ Сгенерировано {len(configs)} конфигурационных файлов в папке '{self.output_dir}'")

    def save_batch_config(self, configs: List[Dict[str, Any]], filename: str = "all_configs.json"):
        """Сохраняет все конфигурации в один файл"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(configs, f, indent=2, ensure_ascii=False)

        print(f"✅ Все конфигурации сохранены в '{filepath}'")

    def generate_statistics(self, configs: List[Dict[str, Any]]):
        """Выводит статистику по сгенерированным конфигурациям"""
        print("\n📊 Статистика сгенерированных конфигураций:")
        print(f"Всего конфигураций: {len(configs)}")

        # Статистика по handlers_amount
        handlers = [c['handlers_amount'] for c in configs]
        print(f"\nКоличество обработчиков:")
        print(f"  Мин: {min(handlers)}, Макс: {max(handlers)}, Среднее: {sum(handlers) / len(handlers):.1f}")

        # Статистика по tasks_amount
        tasks = [c['tasks_amount'] for c in configs]
        print(f"\nКоличество задач:")
        print(f"  Мин: {min(tasks)}, Макс: {max(tasks)}, Среднее: {sum(tasks) / len(tasks):.1f}")

        # Статистика по вероятности ошибок
        errors = [c['temp_error_probability_at_high_load'] for c in configs]
        print(f"\nВероятность ошибок при нагрузке:")
        print(f"  Мин: {min(errors):.3f}, Макс: {max(errors):.3f}, Среднее: {sum(errors) / len(errors):.3f}")


def main():
    generator = ConfigGenerator(output_dir="simulation_configs")

    print("🔧 Генерация конфигурационных файлов...")

    # Выберите один из вариантов:

    # Вариант 1: Полностью случайные конфигурации
    # configs = [generator.generate_random_config(i) for i in range(100)]

    # Вариант 2: Сеточные конфигурации (систематический перебор параметров)
    # configs = generator.generate_grid_configs()

    # Вариант 3: Смесь (рекомендуется) - 50% случайных + 50% систематических
    configs = generator.generate_mixed_configs(count=100)

    # Сохраняем конфигурации
    generator.save_configs(configs, prefix="sim_config")

    # Опционально: сохранить все в один файл
    generator.save_batch_config(configs, filename="all_configs.json")

    # Показываем статистику
    generator.generate_statistics(configs)

    print("\n✨ Готово! Конфигурационные файлы сгенерированы.")


if __name__ == "__main__":
    main()
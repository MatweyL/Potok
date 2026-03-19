"""
Генерация 100 конфигураций для системы имитационного моделирования.
Покрывает широкий спектр сценариев: малые/большие системы, стабильные/нестабильные,
с масштабированием и без, разные нагрузки и таймауты.
"""

import json
import random
from typing import List

from imitation_modelling.schemas import InputHandlerScalingRule, ScaleDirection, SystemParams


# ─── Вспомогательные генераторы ──────────────────────────────────────────────

def make_scaling_rules(handlers_amount: int, scenario: str) -> List[InputHandlerScalingRule]:
    """
    Генерирует правила масштабирования в зависимости от сценария.
    """
    rules = []

    if scenario == "node_failure":
        # Имитация отказа одного узла на середине работы
        rules.append(InputHandlerScalingRule(
            direction=ScaleDirection.DECREASE,
            threshold=round(random.uniform(0.3, 0.6), 2),
            amount=1,
        ))

    elif scenario == "scale_up":
        # Подключение дополнительных обработчиков по мере роста прогресса
        rules.append(InputHandlerScalingRule(
            direction=ScaleDirection.INCREASE,
            threshold=round(random.uniform(0.2, 0.4), 2),
            amount=random.randint(1, max(1, handlers_amount // 3)),
        ))

    elif scenario == "scale_down_then_up":
        # Сначала уменьшаем (обслуживание), потом возвращаем
        drop_threshold = round(random.uniform(0.2, 0.4), 2)
        recover_threshold = round(drop_threshold + random.uniform(0.2, 0.35), 2)
        drop_amount = random.randint(1, max(1, handlers_amount // 2))
        rules.append(InputHandlerScalingRule(
            direction=ScaleDirection.DECREASE,
            threshold=drop_threshold,
            amount=drop_amount,
        ))
        rules.append(InputHandlerScalingRule(
            direction=ScaleDirection.INCREASE,
            threshold=recover_threshold,
            amount=drop_amount,
        ))

    elif scenario == "gradual_scale_up":
        # Поэтапный рост: два шага увеличения
        t1 = round(random.uniform(0.15, 0.35), 2)
        t2 = round(t1 + random.uniform(0.2, 0.35), 2)
        rules.append(InputHandlerScalingRule(direction=ScaleDirection.INCREASE, threshold=t1, amount=1))
        rules.append(InputHandlerScalingRule(direction=ScaleDirection.INCREASE, threshold=t2, amount=1))

    elif scenario == "collapse":
        # Постепенный отказ нескольких узлов
        t1 = round(random.uniform(0.25, 0.45), 2)
        t2 = round(t1 + random.uniform(0.15, 0.3), 2)
        rules.append(InputHandlerScalingRule(direction=ScaleDirection.DECREASE, threshold=t1, amount=1))
        if handlers_amount >= 3:
            rules.append(InputHandlerScalingRule(direction=ScaleDirection.DECREASE, threshold=t2, amount=1))

    # scenario == "stable" → rules остаётся пустым

    return rules


def generate_config(index: int) -> SystemParams:
    rng = random.Random(index)  # воспроизводимость: seed = номер конфига

    # ── Базовые параметры системы ─────────────────────────────────────────────

    # Группы размера системы (чтобы покрыть широкий диапазон)
    size_group = index % 5
    if size_group == 0:  # tiny
        handlers_amount = rng.randint(1, 3)
        handler_max_tasks = rng.randint(2, 5)
        tasks_amount = rng.randint(100, 300)
    elif size_group == 1:  # small
        handlers_amount = rng.randint(3, 6)
        handler_max_tasks = rng.randint(3, 8)
        tasks_amount = rng.randint(300, 800)
    elif size_group == 2:  # medium
        handlers_amount = rng.randint(5, 10)
        handler_max_tasks = rng.randint(5, 12)
        tasks_amount = rng.randint(800, 2000)
    elif size_group == 3:  # large
        handlers_amount = rng.randint(10, 20)
        handler_max_tasks = rng.randint(8, 20)
        tasks_amount = rng.randint(2000, 5000)
    else:  # huge
        handlers_amount = rng.randint(15, 30)
        handler_max_tasks = rng.randint(10, 30)
        tasks_amount = rng.randint(5000, 10000)

    # ── Параметры нагрузки / перегрузки ──────────────────────────────────────

    # Порог перегрузки: строгий (0.7–0.8), умеренный (0.8–0.9), мягкий (0.9–1.0)
    load_profile = index % 3
    if load_profile == 0:
        tasks_part_from_all_for_high_load = round(rng.uniform(0.65, 0.80), 2)
        temp_error_probability_at_high_load = round(rng.uniform(0.3, 0.6), 2)  # высокая вероятность отказа
    elif load_profile == 1:
        tasks_part_from_all_for_high_load = round(rng.uniform(0.80, 0.90), 2)
        temp_error_probability_at_high_load = round(rng.uniform(0.1, 0.3), 2)
    else:
        tasks_part_from_all_for_high_load = round(rng.uniform(0.90, 0.99), 2)
        temp_error_probability_at_high_load = round(rng.uniform(0.01, 0.1), 2)  # низкая вероятность отказа

    # ── Времена выполнения задач ──────────────────────────────────────────────

    exec_left = rng.randint(5, 30)
    exec_right = exec_left + rng.randint(5, 40)  # всегда right > left

    time_step_seconds = rng.randint(5, 30)

    # ── Таймауты (соблюдаем жёсткие ограничения из Field description) ─────────
    #   execution_confirm_timeout < interrupted_timeout
    #   broker_task_ttl < interrupted_timeout  (точнее < таймаут перевода в SUCCEEDED,
    #   но для простоты: broker_task_ttl < interrupted_timeout)

    interrupted_timeout = rng.randint(300, 800)
    # confirm должен быть явно меньше interrupted
    execution_confirm_timeout = rng.randint(
        max(exec_right + time_step_seconds, 50),
        max(interrupted_timeout - 50, interrupted_timeout // 2),
    )
    broker_task_ttl = rng.randint(
        execution_confirm_timeout + 10,
        interrupted_timeout - 10,
    )

    run_timeout = rng.randint(10, 60)
    metric_provider_period = rng.randint(
        run_timeout * 2,  # минимум — хотя бы 2 цикла отправки
        run_timeout * 8,
    )

    # ── Сценарий масштабирования ──────────────────────────────────────────────

    scaling_scenarios = ["stable", "stable", "node_failure", "scale_up",
                         "scale_down_then_up", "gradual_scale_up", "collapse"]
    scenario = scaling_scenarios[index % len(scaling_scenarios)]
    scaling_rules = make_scaling_rules(handlers_amount, scenario)

    # ── Имя конфига ───────────────────────────────────────────────────────────

    size_names = ["tiny", "small", "medium", "large", "huge"]
    load_names = ["strict_load", "moderate_load", "soft_load"]
    config_name = (
        f"cfg_{index:03d}"
        f"__{size_names[size_group]}"
        f"__{load_names[load_profile]}"
        f"__{scenario}"
    )

    return SystemParams(
        handlers_amount=handlers_amount,
        handler_max_tasks=handler_max_tasks,
        execution_confirm_timeout=execution_confirm_timeout,
        tasks_part_from_all_for_high_load=tasks_part_from_all_for_high_load,
        temp_error_probability_at_high_load=temp_error_probability_at_high_load,
        random_timeout_generator_left=exec_left,
        random_timeout_generator_right=exec_right,
        tasks_amount=tasks_amount,
        interrupted_timeout=interrupted_timeout,
        run_timeout=run_timeout,
        metric_provider_period=metric_provider_period,
        time_step_seconds=time_step_seconds,
        broker_task_ttl=broker_task_ttl,
        handler_scaling_rules=scaling_rules,
        config_name=config_name,
        max_run_seconds=180,  # фиксировано для честного сравнения алгоритмов
    )


# ─── Основной блок ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    configs = [generate_config(i).model_dump() for i in range(100)]

    output_path = "simulation_configs.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(configs, f, ensure_ascii=False, indent=2)

    print(f"Сгенерировано {len(configs)} конфигураций → {output_path}")

    # Краткая статистика по сгенерированным конфигам
    scenarios = {}
    sizes = {}
    for c in configs:
        name = c["config_name"]
        parts = name.split("__")
        sz = parts[1]
        scn = parts[3]
        scenarios[scn] = scenarios.get(scn, 0) + 1
        sizes[sz] = sizes.get(sz, 0) + 1

    print("\nРаспределение по сценариям масштабирования:")
    for k, v in sorted(scenarios.items()):
        print(f"  {k:25s}: {v}")

    print("\nРаспределение по размеру системы:")
    for k, v in sorted(sizes.items()):
        print(f"  {k:10s}: {v}")

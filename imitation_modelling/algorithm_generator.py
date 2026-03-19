"""
Генерация конфигураций параметров алгоритмов балансировки.

Для каждой конфигурации системы (SystemParams) создаётся 30 конфигураций
каждого алгоритма (CONSTANT_SIZE / AIMD / MOVING_PID):
  - 10 с минимальным размером батча (batch_size = 1)
  - 10 с оптимальным размером батча (расчёт по параметрам системы)
  - 10 с максимальным размером батча (= tasks_amount)

Итого на одну системную конфигурацию: 30 × 3 = 90 конфигураций алгоритмов.
Общий выход: 100 × 90 = 9000 записей.
"""

import json
import math
import random
from typing import Any, Dict


# ─── Типы ────────────────────────────────────────────────────────────────────

CONSTANT_SIZE = "CONSTANT_SIZE"
AIMD          = "AIMD"
MOVING_PID    = "MOVING_PID"


# ─── Расчёт оптимального размера батча ───────────────────────────────────────

def calc_optimal_batch(system: Dict[str, Any]) -> int:
    """
    max_stable_tasks_count = handlers_count × handler_max_tasks × overload_bound
    if avg_task_duration <= push_tasks_timeout:
        optimal_batch_size = max_stable_tasks_count
    else:
        avg_iters = avg_task_duration / push_tasks_timeout
        optimal_batch_size = max_stable_tasks_count / avg_iters
    """
    avg_task_duration = (
        system["random_timeout_generator_left"] + system["random_timeout_generator_right"]
    ) / 2.0

    max_stable = (
        system["handlers_amount"]
        * system["handler_max_tasks"]
        * system["tasks_part_from_all_for_high_load"]
    )

    if avg_task_duration <= system["run_timeout"]:
        optimal = max_stable
    else:
        avg_iters = avg_task_duration / system["run_timeout"]
        optimal = max_stable / avg_iters

    return max(1, int(optimal))


# ─── Генераторы вариантов параметров ─────────────────────────────────────────

def make_constant_configs(batch_min: int, batch_opt: int, batch_max: int,
                          rng: random.Random) -> list:
    """
    CONSTANT_SIZE — baseline-алгоритм, единственный параметр batch_size.
    Вариации внутри одной точки бессмысленны: 3 конфига на алгоритм.
    """
    return [
        {
            "type": CONSTANT_SIZE,
            "arguments": {"batch_size": batch_min},
            "description": "constant__min_batch",
        },
        {
            "type": CONSTANT_SIZE,
            "arguments": {"batch_size": batch_opt},
            "description": "constant__opt_batch",
        },
        {
            "type": CONSTANT_SIZE,
            "arguments": {"batch_size": batch_max},
            "description": "constant__max_batch",
        },
    ]


def make_aimd_configs(batch_min: int, batch_opt: int, batch_max: int,
                      rng: random.Random) -> list:
    """
    AIMD параметры:
      delta        — аддитивный прирост (целое, 1..32)
      beta         — мультипликативное снижение (0.1..0.9)
      base_batch_size — стартовый размер батча
      batch_size_min  — нижняя граница
      batch_size_max  — верхняя граница
    """
    configs = []

    def _aimd_variant(base: int, description_prefix: str, var_idx: int) -> Dict:
        delta        = rng.randint(1, 32)
        beta         = round(rng.uniform(0.1, 0.9), 3)
        # min/max вокруг base: min=base//4..base//2, max=base..base*4
        bs_min = max(1, rng.randint(max(1, base // 4), max(1, base // 2)))
        bs_max = rng.randint(max(bs_min + 1, base), max(bs_min + 2, base * 4))
        return {
            "type": AIMD,
            "arguments": {
                "base_batch_size": base,
                "delta":           delta,
                "beta":            beta,
                "batch_size_min":  bs_min,
                "batch_size_max":  bs_max,
            },
            "description": f"aimd__{description_prefix}__var{var_idx:02d}",
        }

    for i in range(10):
        configs.append(_aimd_variant(batch_min, "min_batch", i + 1))

    for i in range(10):
        configs.append(_aimd_variant(batch_opt, "opt_batch", i + 1))

    for i in range(10):
        configs.append(_aimd_variant(batch_max, "max_batch", i + 1))

    return configs


def make_moving_pid_configs(batch_min: int, batch_opt: int, batch_max: int,
                            rng: random.Random) -> list:
    """
    MOVING_PID параметры:
      cold_start_batch_size         — стартовый размер батча (0 = авто)
      cold_start_growth_multiplier  — множитель роста на cold start (1.1..3.0)
      range_retention_iterations    — сколько итераций держим диапазон (2..8)
      adjustment_grow_multiplier    — сдвиг вправо при росте throughput (1.1..2.5)
      adjustment_shrink_multiplier  — сдвиг влево при насыщении (0.5..0.95)
      throughput_growth_threshold   — порог роста EMA (0.02..0.20)
    """
    configs = []

    def _pid_variant(cold_start_base: int, description_prefix: str, var_idx: int) -> Dict:
        return {
            "type": MOVING_PID,
            "arguments": {
                "cold_start_batch_size":        cold_start_base,
                "cold_start_growth_multiplier": round(rng.uniform(1.1, 3.0), 3),
                "range_retention_iterations":   rng.randint(2, 8),
                "adjustment_grow_multiplier":   round(rng.uniform(1.1, 2.5), 3),
                "adjustment_shrink_multiplier": round(rng.uniform(0.5, 0.95), 3),
                "throughput_growth_threshold":  round(rng.uniform(0.02, 0.20), 3),
            },
            "description": f"moving_pid__{description_prefix}__var{var_idx:02d}",
        }

    for i in range(10):
        configs.append(_pid_variant(batch_min, "min_batch", i + 1))

    for i in range(10):
        configs.append(_pid_variant(batch_opt, "opt_batch", i + 1))

    for i in range(10):
        configs.append(_pid_variant(batch_max, "max_batch", i + 1))

    return configs


# ─── Основной генератор ───────────────────────────────────────────────────────

def generate_algo_configs_for_system(system: Dict[str, Any]) -> list:
    """
    Возвращает список из 90 конфигураций алгоритмов для одной системной
    конфигурации (30 на каждый из трёх алгоритмов).
    """
    # Воспроизводимый RNG: seed = config_name, чтобы независимо пересчитывать
    rng = random.Random(system["config_name"])

    batch_min = 1
    batch_opt = calc_optimal_batch(system)
    batch_max = system["tasks_amount"]

    result = []

    for cfg in make_constant_configs(batch_min, batch_opt, batch_max, rng):
        cfg["system_config_name"] = system["config_name"]
        cfg["batch_min"] = batch_min
        cfg["batch_opt"] = batch_opt
        cfg["batch_max"] = batch_max
        result.append(cfg)

    for cfg in make_aimd_configs(batch_min, batch_opt, batch_max, rng):
        cfg["system_config_name"] = system["config_name"]
        cfg["batch_min"] = batch_min
        cfg["batch_opt"] = batch_opt
        cfg["batch_max"] = batch_max
        result.append(cfg)

    for cfg in make_moving_pid_configs(batch_min, batch_opt, batch_max, rng):
        cfg["system_config_name"] = system["config_name"]
        cfg["batch_min"] = batch_min
        cfg["batch_opt"] = batch_opt
        cfg["batch_max"] = batch_max
        result.append(cfg)

    return result


# ─── Точка входа ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    with open("simulation_configs.json", encoding="utf-8") as f:
        system_configs = json.load(f)

    all_algo_configs = []
    for system in system_configs:
        all_algo_configs.extend(generate_algo_configs_for_system(system))

    output_path = "algo_params.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_algo_configs, f, ensure_ascii=False, indent=2)

    print(f"Сгенерировано {len(all_algo_configs)} конфигураций алгоритмов → {output_path}")
    print(f"  На одну системную конфигурацию: {len(all_algo_configs) // len(system_configs)}")

    # ── Статистика ────────────────────────────────────────────────────────────
    from collections import Counter
    types = Counter(c["type"] for c in all_algo_configs)
    print("\nПо типу алгоритма:")
    for t, n in sorted(types.items()):
        print(f"  {t:20s}: {n}")

    batches = Counter(
        "min"  if c["arguments"].get("batch_size", c["arguments"].get("base_batch_size", c["arguments"].get("cold_start_batch_size", 0))) == 1
        else "max" if c["arguments"].get("batch_size", c["arguments"].get("base_batch_size", c["arguments"].get("cold_start_batch_size", -1))) == c["batch_max"]
        else "opt"
        for c in all_algo_configs
    )
    print("\nПо стартовому размеру батча:")
    for b, n in sorted(batches.items()):
        print(f"  {b}: {n}")

    # ── Проверка инвариантов ──────────────────────────────────────────────────
    for c in all_algo_configs:
        if c["type"] == AIMD:
            a = c["arguments"]
            assert a["batch_size_min"] < a["batch_size_max"], f"AIMD min>=max: {c}"
            assert 0 < a["beta"] < 1, f"AIMD beta out of range: {c}"
            assert a["delta"] >= 1, f"AIMD delta < 1: {c}"
        if c["type"] == MOVING_PID:
            a = c["arguments"]
            assert a["cold_start_growth_multiplier"] > 1, c
            assert a["adjustment_grow_multiplier"] > 1, c
            assert 0 < a["adjustment_shrink_multiplier"] < 1, c
    print("\nВсе инварианты OK")

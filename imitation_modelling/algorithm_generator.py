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

CONSTANT_SIZE    = "CONSTANT_SIZE"
AIMD             = "AIMD"
MOVING_PID       = "MOVING_PID"
MOVING_PID_V2    = "MOVING_PID_V2"
GRADIENT_ASCENT  = "GRADIENT_ASCENT"
ADAPTIVE_MODEL   = "ADAPTIVE_MODEL"


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
        # batch_size_min всегда 1 — иначе симуляция может быть очень долгой при перегрузке
        bs_min = 1
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



def make_moving_pid_v2_configs(batch_min: int, batch_opt: int, batch_max: int,
                               rng: random.Random) -> list:
    """
    MOVING_PID_V2 параметры:
      cold_start_batch_size           — стартовый размер батча
      cold_start_growth_multiplier    — множитель роста на cold start (1.1..2.0)
      range_retention_iterations      — итераций удержания диапазона (2..6)
      adjustment_grow_multiplier      — базовый сдвиг вправо (1.1..1.6)
                                        намеренно уже чем в v1 — адаптация снаружи
      adjustment_shrink_multiplier    — сдвиг влево при насыщении (0.75..0.95)
      adjustment_shrink_on_overload   — агрессивное сжатие при перегрузке (0.4..0.75)
      grow_penalty_factor             — штраф grow после перегрузки (0.6..0.9)
      grow_recovery_factor            — восстановление grow после стаб. цикла (1.01..1.1)
      grow_multiplier_min             — нижний предел grow (1.02..1.1)
      overload_ceiling_safety_margin  — отступ от потолка перегрузки (0.75..0.95)
      overload_ceiling_forget_after   — циклов до сброса потолка (3..10)
      min_batch_floor                 — всегда 1
      min_range_width                 — минимальная ширина диапазона (2..8)
      throughput_growth_threshold     — порог роста EMA (0.02..0.15)
      throughput_ema_alpha            — сглаживание EMA (0.2..0.5)
    """
    configs = []

    def _v2_variant(cold_start_base: int, description_prefix: str, var_idx: int) -> Dict:
        grow_base = round(rng.uniform(1.1, 1.6), 3)
        return {
            "type": MOVING_PID_V2,
            "arguments": {
                "cold_start_batch_size":          cold_start_base,
                "cold_start_growth_multiplier":   round(rng.uniform(1.1, 2.0), 3),
                "range_retention_iterations":     rng.randint(2, 6),
                "adjustment_grow_multiplier":     grow_base,
                "adjustment_shrink_multiplier":   round(rng.uniform(0.75, 0.95), 3),
                "adjustment_shrink_on_overload":  round(rng.uniform(0.4, 0.75), 3),
                "grow_penalty_factor":            round(rng.uniform(0.6, 0.9), 3),
                "grow_recovery_factor":           round(rng.uniform(1.01, 1.1), 3),
                "grow_multiplier_min":            round(rng.uniform(1.02, min(1.1, grow_base - 0.01)), 3),
                "overload_ceiling_safety_margin": round(rng.uniform(0.75, 0.95), 3),
                "overload_ceiling_forget_after":  rng.randint(3, 10),
                "min_batch_floor":                1,   # всегда 1
                "min_range_width":                rng.randint(2, 8),
                "throughput_growth_threshold":    round(rng.uniform(0.02, 0.15), 3),
                "throughput_ema_alpha":           round(rng.uniform(0.2, 0.5), 3),
            },
            "description": f"moving_pid_v2__{description_prefix}__var{var_idx:02d}",
        }

    for i in range(10):
        configs.append(_v2_variant(batch_min, "min_batch", i + 1))

    for i in range(10):
        configs.append(_v2_variant(batch_opt, "opt_batch", i + 1))

    for i in range(10):
        configs.append(_v2_variant(batch_max, "max_batch", i + 1))

    return configs



def make_gradient_ascent_configs(batch_min: int, batch_opt: int, batch_max: int,
                                 rng: random.Random) -> list:
    """
    GradientAscentProvider параметры:
      cold_start_batch_size          — стартовый батч
      cold_start_growth_multiplier   — множитель роста cold start (1.5..3.0)
      learning_rate                  — скорость обучения (1..20)
      gradient_ema_alpha             — сглаживание градиента (0.1..0.5)
      max_step_fraction              — макс. шаг как доля от batch_size (0.1..0.5)
      min_exploration_step           — минимальный шаг исследования, всегда 1
      overload_shrink_factor         — сжатие при перегрузке (0.5..0.85)
      batch_size_min                 — всегда 1
    """
    configs = []

    def _grad_variant(cold_start_base: int, description_prefix: str, var_idx: int) -> Dict:
        return {
            "type": GRADIENT_ASCENT,
            "arguments": {
                "cold_start_batch_size":        cold_start_base,
                "cold_start_growth_multiplier": round(rng.uniform(1.5, 3.0), 3),
                "learning_rate":                round(rng.uniform(1.0, 20.0), 2),
                "gradient_ema_alpha":           round(rng.uniform(0.1, 0.5), 3),
                "max_step_fraction":            round(rng.uniform(0.1, 0.5), 3),
                "min_exploration_step":         1,    # всегда 1
                "overload_shrink_factor":       round(rng.uniform(0.5, 0.85), 3),
                "batch_size_min":               1,    # всегда 1
            },
            "description": f"gradient_ascent__{description_prefix}__var{var_idx:02d}",
        }

    for i in range(10):
        configs.append(_grad_variant(batch_min, "min_batch", i + 1))
    for i in range(10):
        configs.append(_grad_variant(batch_opt, "opt_batch", i + 1))
    for i in range(10):
        configs.append(_grad_variant(batch_max, "max_batch", i + 1))

    return configs


def make_adaptive_model_configs(batch_min: int, batch_opt: int, batch_max: int,
                                rng: random.Random) -> list:
    """
    AdaptiveModelProvider параметры:
      cold_start_batch_size           — стартовый батч
      cold_start_growth_multiplier    — множитель роста cold start (1.5..3.0)
      model_alpha_low                 — EMA нижней зоны модели (0.1..0.35)
      model_alpha_peak                — EMA пиковой зоны (0.2..0.5)
      model_alpha_high                — EMA верхней зоны (0.3..0.7), агрессивнее
      throughput_ema_alpha            — сглаживание throughput (0.1..0.5)
      exploration_interval            — итераций между эксплорациями (3..10)
      exploration_step_fraction       — шаг эксплорации как доля (B_high-B_low) (0.05..0.3)
      min_model_width                 — минимальная ширина модели (2..8)
      overload_rate_threshold         — порог доли отказов = перегрузка (0.05..0.3)
    """
    configs = []

    def _adaptive_variant(cold_start_base: int, description_prefix: str, var_idx: int) -> Dict:
        alpha_low  = round(rng.uniform(0.10, 0.35), 3)
        alpha_peak = round(rng.uniform(0.20, 0.50), 3)
        # alpha_high всегда > alpha_peak — перегрузку замечаем быстрее
        alpha_high = round(rng.uniform(max(alpha_peak + 0.05, 0.30), 0.70), 3)
        return {
            "type": ADAPTIVE_MODEL,
            "arguments": {
                "cold_start_batch_size":        cold_start_base,
                "cold_start_growth_multiplier": round(rng.uniform(1.5, 3.0), 3),
                "model_alpha_low":              alpha_low,
                "model_alpha_peak":             alpha_peak,
                "model_alpha_high":             alpha_high,
                "throughput_ema_alpha":         round(rng.uniform(0.1, 0.5), 3),
                "exploration_interval":         rng.randint(3, 10),
                "exploration_step_fraction":    round(rng.uniform(0.05, 0.30), 3),
                "min_model_width":              rng.randint(2, 8),
                "overload_rate_threshold":      round(rng.uniform(0.05, 0.30), 3),
            },
            "description": f"adaptive_model__{description_prefix}__var{var_idx:02d}",
        }

    for i in range(10):
        configs.append(_adaptive_variant(batch_min, "min_batch", i + 1))
    for i in range(10):
        configs.append(_adaptive_variant(batch_opt, "opt_batch", i + 1))
    for i in range(10):
        configs.append(_adaptive_variant(batch_max, "max_batch", i + 1))

    return configs


# ─── Основной генератор ───────────────────────────────────────────────────────

def generate_algo_configs_for_system(system: Dict[str, Any]) -> list:
    """
    Возвращает список из 153 конфигураций алгоритмов для одной системной
    конфигурации:
    3 CONSTANT_SIZE + 30 AIMD + 30 MOVING_PID + 30 MOVING_PID_V2
    + 30 GRADIENT_ASCENT + 30 ADAPTIVE_MODEL.
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

    for cfg in make_moving_pid_v2_configs(batch_min, batch_opt, batch_max, rng):
        cfg["system_config_name"] = system["config_name"]
        cfg["batch_min"] = batch_min
        cfg["batch_opt"] = batch_opt
        cfg["batch_max"] = batch_max
        result.append(cfg)

    for cfg in make_gradient_ascent_configs(batch_min, batch_opt, batch_max, rng):
        cfg["system_config_name"] = system["config_name"]
        cfg["batch_min"] = batch_min
        cfg["batch_opt"] = batch_opt
        cfg["batch_max"] = batch_max
        result.append(cfg)

    for cfg in make_adaptive_model_configs(batch_min, batch_opt, batch_max, rng):
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
        if c["type"] == MOVING_PID_V2:
            a = c["arguments"]
            assert a["cold_start_growth_multiplier"] > 1, c
            assert a["adjustment_grow_multiplier"] > 1, c
            assert 0 < a["adjustment_shrink_multiplier"] < 1, c
            assert 0 < a["adjustment_shrink_on_overload"] < 1, c
            assert a["adjustment_shrink_on_overload"] < a["adjustment_shrink_multiplier"], c
            assert 0 < a["grow_penalty_factor"] < 1, c
            assert a["grow_recovery_factor"] > 1, c
            assert a["grow_multiplier_min"] < a["adjustment_grow_multiplier"], c
            assert a["min_batch_floor"] == 1, c
            assert 0 < a["overload_ceiling_safety_margin"] < 1, c
        if c["type"] == GRADIENT_ASCENT:
            a = c["arguments"]
            assert a["batch_size_min"] == 1, f"GRADIENT_ASCENT batch_size_min != 1: {c}"
            assert a["min_exploration_step"] == 1, f"GRADIENT_ASCENT min_exploration_step != 1: {c}"
            assert a["cold_start_growth_multiplier"] > 1, c
            assert 0 < a["overload_shrink_factor"] < 1, c
            assert 0 < a["max_step_fraction"] < 1, c
        if c["type"] == ADAPTIVE_MODEL:
            a = c["arguments"]
            assert a["cold_start_growth_multiplier"] > 1, c
            assert a["model_alpha_high"] > a["model_alpha_peak"],                 f"ADAPTIVE_MODEL alpha_high должен быть > alpha_peak: {c}"
            assert 0 < a["overload_rate_threshold"] < 1, c
            assert 0 < a["exploration_step_fraction"] < 1, c
    print("\nВсе инварианты OK")
"""
find_best_adaptive.py
Находит лучшие параметры ADAPTIVE_MODEL.

Методика:
  На каждой из 100 конфигураций системы сравниваем все варианты
  ADAPTIVE_MODEL между собой (по взвешенному score).
  Победитель — вариант с минимальным score.
  Итог — какие значения параметров чаще всего встречались у победителей.

Использование:
    from find_best_adaptive import find_best_adaptive, compute_metrics_with_sys_key
    records = [compute_metrics_with_sys_key(r) for r in runs]
    records = remove_outliers_iqr(records)
    find_best_adaptive(records)
"""

import statistics
from collections import Counter, defaultdict

TARGET_ALGO = "ADAPTIVE_MODEL"

SCORE_METRICS = [
    ("tries_mean",     False, 3.0),
    ("overload_count", False, 2.0),
    ("avg_util",       True,  1.0),
    ("total_time",     False, 1.0),
]

PERF_METRICS = [
    ("tries_mean",     "Среднее попыток/задача", ".2f", False),
    ("tries_median",   "Медиана попыток",        ".0f", False),
    ("first_try_rate", "Доля с 1-й попытки, %",  ".1f", True),
    ("overload_count", "Перегрузок (шт.)",        ".0f", False),
    ("avg_util",       "Утилизация (0–1)",         ".3f", False),
    ("total_time",     "Вирт. время (сек)",        ".2f", False),
]


def _sys_key(run_params: dict) -> str:
    sp = run_params.get("system_params", {})
    return "|".join(sorted(
        f"{k}={v}" for k, v in sp.items()
        if isinstance(v, (int, float, str, bool))
    ))


def _algo_config_key(record: dict) -> str:
    return record["description"]


def compute_metrics_with_sys_key(run: dict) -> dict:
    from visualization_v3 import compute_metrics
    record = compute_metrics(run)
    record["sys_key"] = _sys_key(run["params"])
    algo_p = dict(run["params"].get("task_batch_provider_params", {}))
    for drop in ("type", "description"):
        algo_p.pop(drop, None)
    record["algo_params"] = algo_p
    return record


def _weighted_score(recs: list[dict]) -> float:
    """Взвешенный score одного варианта — медиана по его прогонам."""
    result = {}
    for key, _, _ in SCORE_METRICS:
        vals = [r[key] for r in recs if r.get(key) is not None]
        result[key] = statistics.median(vals) if vals else None
    return result


def _param_stats_block(winning_records: list[dict], all_param_keys: list[str], W: int) -> None:
    """Выводит статистику параметров по выигравшим прогонам."""
    print(f"\n  ┌─ ПАРАМЕТРЫ ПОБЕДИТЕЛЕЙ {'─' * (W - 26)}┐")
    for pk in all_param_keys:
        vals = [r["algo_params"][pk]
                for r in winning_records
                if pk in r.get("algo_params", {})]
        if not vals:
            continue

        nums = []
        for v in vals:
            try:
                nums.append(float(v))
            except (TypeError, ValueError):
                pass

        label = f"  │  {pk:<28}"

        if len(nums) == len(vals):
            if all(v == nums[0] for v in nums):
                print(f"{label}  {nums[0]}  (константа, N={len(nums)})")
                continue
            s = sorted(nums)
            n = len(s)
            med  = statistics.median(s)
            mean = statistics.mean(s)
            sd   = statistics.stdev(s) if n > 1 else 0.0
            p25  = s[max(0, int(n * 0.25))]
            p75  = s[min(n - 1, int(n * 0.75))]
            print(f"{label}  median={med:.4g}  mean={mean:.4g}  sd={sd:.4g}"
                  f"  [p25={p25:.4g} – p75={p75:.4g}]"
                  f"  min={s[0]:.4g}  max={s[-1]:.4g}  N={n}")
        else:
            cnt = Counter(str(v) for v in vals)
            total = sum(cnt.values())
            parts = [f"{val}: {c} ({100*c/total:.0f}%)"
                     for val, c in cnt.most_common()]
            print(f"{label}  {'  |  '.join(parts)}")

    print(f"  └{'─' * (W - 2)}┘")


def find_best_adaptive(records: list[dict], top_n: int = 5) -> None:
    """
    Среди прогонов ADAPTIVE_MODEL находит варианты конфигурации,
    которые чаще всего побеждали на разных конфигурациях системы.
    Выводит топ-N + полную статистику параметров и метрик.
    """
    # Фильтруем только ADAPTIVE_MODEL
    am_records = [r for r in records if r["algo_type"] == TARGET_ALGO]
    if not am_records:
        print(f"Нет прогонов {TARGET_ALGO} в данных.")
        return

    has_sys_key = "sys_key" in am_records[0]
    sys_key_fn  = (lambda r: r["sys_key"]) if has_sys_key else (
        lambda r: r.get("run_name", "default").rsplit("__", 1)[0]
    )

    # Группируем: sys_key → description → записи
    grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in am_records:
        grouped[sys_key_fn(r)][_algo_config_key(r)].append(r)

    # На каждой конфигурации системы выбираем победителя
    wins:        dict[str, int]        = defaultdict(int)
    totals:      dict[str, int]        = defaultdict(int)
    win_records: dict[str, list[dict]] = defaultdict(list)  # desc → все выигравшие прогоны

    n_contested = 0
    for sk, cfg_map in grouped.items():
        if len(cfg_map) < 2:
            continue
        n_contested += 1

        # Медианы метрик для каждого варианта
        medians = {}
        for desc, recs in cfg_map.items():
            totals[desc] += 1
            mv: dict[str, list] = defaultdict(list)
            for r in recs:
                for key, _, _ in SCORE_METRICS:
                    v = r.get(key)
                    if v is not None:
                        mv[key].append(v)
            medians[desc] = {k: statistics.median(v) if v else None
                             for k, v in mv.items()}

        # Нормализованный взвешенный score
        final: dict[str, float] = {desc: 0.0 for desc in cfg_map}
        for key, higher_is_better, weight in SCORE_METRICS:
            vals = {d: medians[d][key] for d in cfg_map
                    if medians[d].get(key) is not None}
            if len(vals) < 2:
                continue
            vmin, vmax = min(vals.values()), max(vals.values())
            rng = vmax - vmin if vmax != vmin else 1.0
            for desc, v in vals.items():
                norm = (v - vmin) / rng
                if higher_is_better:
                    norm = 1.0 - norm
                final[desc] += norm * weight

        winner = min(final, key=final.__getitem__)
        wins[winner] += 1
        win_records[winner].extend(cfg_map[winner])

    # Сортируем по убыванию побед
    ranked = sorted(
        {_algo_config_key(r) for r in am_records},
        key=lambda d: (-wins.get(d, 0), d),
    )

    # Все параметры алгоритма
    all_param_keys = sorted({
        k for r in am_records
        for k in r.get("algo_params", {}).keys()
    })

    W   = 120
    SEP = "─" * W
    EQ  = "═" * W

    print(f"\n{EQ}")
    print(f"  ЛУЧШИЕ ПАРАМЕТРЫ: {TARGET_ALGO}")
    print(f"  Прогонов: {len(am_records)}   |"
          f"  Уникальных конфигураций: {len(ranked)}   |"
          f"  Конфигураций системы с конкуренцией: {n_contested}")
    print(EQ)

    # ── Рейтинговая таблица ───────────────────────────────────────────────────
    print(f"\n  {'#':<4} {'Описание конфигурации':<60} "
          f"{'Побед':>7}  {'Участий':>8}  {'Win%':>7}")
    print(SEP)
    for rank, desc in enumerate(ranked[:top_n], 1):
        w   = wins.get(desc, 0)
        t   = totals.get(desc, 0)
        pct = 100 * w / t if t else 0.0
        bar = "█" * max(1, int(pct / 5)) if w > 0 else ""
        desc_s = (desc[:57] + "…") if len(desc) > 60 else desc
        print(f"  {rank:<4} {desc_s:<60} {w:>7}  {t:>8}  {pct:>6.1f}%  {bar}")
    print(SEP)

    # ── Детальная карточка для каждого из топ-N ───────────────────────────────
    for rank, desc in enumerate(ranked[:top_n], 1):
        w   = wins.get(desc, 0)
        t   = totals.get(desc, 0)
        pct = 100 * w / t if t else 0.0
        w_recs = win_records.get(desc, [])  # прогоны, где этот вариант победил
        a_recs = [r for r in am_records if _algo_config_key(r) == desc]  # все прогоны

        print(f"\n{EQ}")
        print(f"  #{rank}  Побед: {w}/{t} ({pct:.1f}%)")
        print(f"  {desc}")
        print(EQ)

        # Параметры (статистика по ВСЕМ прогонам этого варианта)
        if all_param_keys and a_recs and "algo_params" in a_recs[0]:
            _param_stats_block(a_recs, all_param_keys, W)

        # Метрики: две колонки — все прогоны vs только победные прогоны
        print(f"\n  ┌─ МЕТРИКИ {'─' * (W - 12)}┐")
        hdr = (f"  │  {'Метрика':<30}  "
               f"{'Все прогоны (median / mean / sd)':^38}  "
               f"{'Победные прогоны (median / mean / sd)':^38}  N_all  N_win")
        print(hdr)
        print(f"  │  {'─' * (W - 6)}")

        for key, label, fmt, pct_flag in PERF_METRICS:
            all_v = [r[key] for r in a_recs if r.get(key) is not None]
            win_v = [r[key] for r in w_recs if r.get(key) is not None]

            if pct_flag:
                all_v = [v * 100 for v in all_v]
                win_v = [v * 100 for v in win_v]

            def _smry(vals):
                if not vals:
                    return f"{'—':^38}"
                med  = statistics.median(vals)
                mean = statistics.mean(vals)
                sd   = statistics.stdev(vals) if len(vals) > 1 else 0.0
                return f"{med:{fmt}} / {mean:{fmt}} / {sd:{fmt}}".center(38)

            print(f"  │  {label:<30}  {_smry(all_v)}  {_smry(win_v)}"
                  f"  {len(all_v):>5}  {len(win_v):>5}")

        print(f"  └{'─' * (W - 2)}┘")

    # ── Итог: самые частые значения параметров у победителей ─────────────────
    all_win_recs = [r for desc in ranked[:top_n]
                    for r in win_records.get(desc, [])]

    if all_win_recs and all_param_keys:
        print(f"\n{EQ}")
        print(f"  РЕКОМЕНДУЕМЫЕ ЗНАЧЕНИЯ ПАРАМЕТРОВ")
        print(f"  (статистика по прогонам топ-{top_n} победителей вместе, N={len(all_win_recs)})")
        print(EQ)
        _param_stats_block(all_win_recs, all_param_keys, W)

    print(f"\n{EQ}\n")
"""
Графики для анализа запусков разных конфигураций алгоритмов на одной системе.

Читает сырые JSON-файлы результатов симуляции из INPUT_DIR.

Фильтрация (применяется ко всем кроме CONSTANT_SIZE):
  - duration < MAX_DURATION_SEC (прогон завершился вовремя)
  - хотя бы один шаг истории с batch_size > 1 (алгоритм не застрял на дне)

Выходные файлы (OUTPUT_DIR):
  1. utilization.html      — boxplot avg_utilization по алгоритмам
  2. overloads.html        — boxplot overload_count по алгоритмам
  3. total_time.html       — boxplot total_time по алгоритмам
  4. retries.html          — boxplot tries_mean по алгоритмам
  5. cdf_tries.html        — CDF распределения попыток (агрегат по прогонам)
  6. heatmap.html          — heatmap algo × описание конфига, метрика на выбор
"""

import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

import plotly.graph_objects as go

try:
    from tqdm import tqdm
except ImportError:
    print("Устанавливаю tqdm...")
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm", "-q"])
    from tqdm import tqdm

# ── Настройки ─────────────────────────────────────────────────────────────────

INPUT_DIR = r"D:\University\MAG\Diploma\simulation_results.tar\simulation_results"  # ← папка с сырыми JSON результатами симуляции
OUTPUT_DIR = "./plots_2026_05_16"
MAX_DURATION_SEC = 30.0  # фильтр: прогон завершился за N реальных секунд
HEATMAP_METRIC = "total_time"  # метрика для heatmap: total_time / overload_count / avg_utilization

ALGO_COLORS = {
    "CONSTANT_SIZE": "#636EFA",
    "AIMD": "#EF553B",
    "MOVING_PID": "#00CC96",
    "MOVING_PID_V2": "#AB63FA",
    "GRADIENT_ASCENT": "#FFA15A",
    "ADAPTIVE_MODEL": "#19D3F3",
}
ALGO_ORDER = ["CONSTANT_SIZE", "AIMD", "MOVING_PID", "MOVING_PID_V2",
              "GRADIENT_ASCENT", "ADAPTIVE_MODEL"]

TARGET_ALGOS = {"AIMD", "CONSTANT_SIZE", "ADAPTIVE_MODEL", "GRADIENT_ASCENT"}

# Метрики, по которым проверяем выбросы (IQR-метод, отдельно внутри каждого алгоритма)
OUTLIER_METRICS = ["overload_count", "avg_util", "total_time", "tries_mean"]

IQR_FACTOR = {
    "total_time":      3.0,
    "overload_count":  2.0,
    "avg_util":        1.5,
    "tries_mean": 1.5,
}


# ── Загрузка и фильтрация ─────────────────────────────────────────────────────

def load_runs_via_yield(input_dir: str):
    all_paths = sorted(Path(input_dir).glob("*.json"))
    total_files = len(all_paths)

    counters = {"loaded": 0, "skipped": 0, "passed_filter": 0, "dropped_filter": 0}

    bar = tqdm(
        all_paths,
        total=total_files,
        unit="файл",
        desc="Чтение",
        dynamic_ncols=True,
    )

    for path in bar:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if data["params"]["task_batch_provider_params"]["type"] not in TARGET_ALGOS:
                raise ValueError("not target algo")
            counters["loaded"] += 1

            if passes_filter(data):
                counters["passed_filter"] += 1
                bar.set_postfix(
                    загружено=counters["loaded"],
                    прошло=counters["passed_filter"],
                    отброшено=counters["dropped_filter"],
                    пропущено=counters["skipped"],
                )
                yield data
            else:
                counters["dropped_filter"] += 1

        except Exception as e:
            counters["skipped"] += 1
            bar.set_postfix(
                загружено=counters["loaded"],
                прошло=counters["passed_filter"],
                отброшено=counters["dropped_filter"],
                пропущено=counters["skipped"],
            )

    bar.close()
    print(
        f"\n── Итог чтения ──────────────────────────────\n"
        f"  Всего файлов:        {total_files}\n"
        f"  Целевой алгоритм:    {counters['loaded']}\n"
        f"  Прошло фильтр:       {counters['passed_filter']}\n"
        f"  Отброшено фильтром:  {counters['dropped_filter']}\n"
        f"  Пропущено (ошибки):  {counters['skipped']}\n"
        f"─────────────────────────────────────────────\n"
    )


def passes_filter(run: dict) -> bool:
    """
    Фильтр для адаптивных алгоритмов (CONSTANT_SIZE пропускаем без фильтра):
      1. duration < MAX_DURATION_SEC
      2. хотя бы один шаг с batch_size > 1
    """
    algo_type = run["params"]["task_batch_provider_params"]["type"]
    if algo_type == "CONSTANT_SIZE":
        return True
    if run.get("duration", math.inf) >= MAX_DURATION_SEC:
        return False
    history = run.get("history", [])
    if not any(step.get("batch_size", 0) > 1 for step in history):
        return False
    return True


def compute_metrics(run: dict) -> dict:
    """Вычисляет метрики из сырого JSON прогона."""
    history = run.get("history", [])
    params = run["params"]
    sys_p = params["system_params"]
    algo_p = params["task_batch_provider_params"]
    tries_dist_raw = run.get("succeed_tasks_count_by_tries_count", {})
    tries_dist = {int(k): v for k, v in tries_dist_raw.items()}

    algo_type = algo_p["type"]
    description = algo_p.get("description", "")
    batch_opt = algo_p.get("batch_opt", 0)
    total_tasks = sys_p.get("tasks_amount", 0)

    # Завершённость и время
    last = history[-1] if history else {}
    all_done = last.get("completed", 0) >= total_tasks
    completion_step = next((s for s in history if s.get("completed", 0) >= total_tasks), None)
    total_time = completion_step["time"] if completion_step else None

    # Перегрузки
    overload_count = 0
    for i in range(1, len(history)):
        rf = history[i].get("returnFrequency", 0.0)
        if rf > 0.5:
            overload_count += 1

    # Утилизация
    max_cap = sys_p.get("handlers_amount", 1) * sys_p.get("handler_max_tasks", 1)
    active = [s for s in history if s.get("executionCount", 0) > 0
              or s.get("queuedCount", 0) > 0]
    avg_util = (statistics.mean(s["executionCount"] / max_cap for s in active)
                if active else None)

    # Попытки
    total_task_count = sum(tries_dist.values())
    total_tries = sum(k * v for k, v in tries_dist.items())
    tries_mean = total_tries / total_task_count if total_task_count else None
    tries_max = max(tries_dist.keys()) if tries_dist else None

    # Доля задач, выполненных с первой попытки
    first_try_rate = (tries_dist.get(1, 0) / total_task_count
                      if total_task_count else None)

    # Медиана попыток — корректный перебор CDF
    tries_median = None
    if tries_dist and total_task_count:
        cumulative = 0
        half = total_task_count / 2
        for t in sorted(tries_dist.keys()):
            cumulative += tries_dist[t]
            if cumulative >= half:
                tries_median = t
                break
    return {
        "run_name": run.get("run_name", ""),
        "algo_type": algo_type,
        "description": description,
        "batch_opt": batch_opt,
        "duration": run.get("duration"),
        "all_done": all_done,
        "total_time": total_time,
        "overload_count": overload_count,
        "avg_util": avg_util,
        "tries_mean": tries_mean,
        "tries_median": tries_median,
        "tries_max": tries_max,
        "tries_dist": tries_dist,
        "first_try_rate": first_try_rate,
        "total_task_count": total_task_count,
    }


def remove_outliers_iqr(records: list[dict]) -> list[dict]:
    """
    Удаляет прогоны-выбросы по IQR-методу (±IQR_FACTOR×IQR).
    Границы считаются отдельно для каждого алгоритма, чтобы не смешивать
    их распределения. Прогон отбрасывается целиком, если хотя бы одна
    метрика из OUTLIER_METRICS выходит за границы своего алгоритма.
    """
    # Для каждого алгоритма вычисляем границы по каждой метрике
    algo_bounds: dict[str, dict[str, tuple[float, float]]] = {}
    algos = {r["algo_type"] for r in records}

    for algo in algos:
        algo_bounds[algo] = {}
        algo_records = [r for r in records if r["algo_type"] == algo]
        for metric in OUTLIER_METRICS:
            vals = sorted(v for r in algo_records
                          if (v := r.get(metric)) is not None)
            if len(vals) < 4:
                # Слишком мало точек — не фильтруем эту метрику
                continue
            n = len(vals)
            q1 = vals[n // 4]
            q3 = vals[(3 * n) // 4]
            iqr = q3 - q1
            iqr_factor = IQR_FACTOR.get(metric, 1.5)
            lo = q1 - iqr_factor * iqr
            hi = q3 + iqr_factor * iqr
            algo_bounds[algo][metric] = (lo, hi)

    kept, dropped = [], []
    for r in records:
        bounds = algo_bounds.get(r["algo_type"], {})
        drop_reason = None
        for metric, (lo, hi) in bounds.items():
            v = r.get(metric)
            if v is not None and not (lo <= v <= hi):
                drop_reason = metric
                break
        if drop_reason is not None:
            r["_drop_reason"] = drop_reason
            dropped.append(r)
        else:
            kept.append(r)

    # Сводка по алгоритмам
    from collections import Counter
    drop_counts = Counter(r["algo_type"] for r in dropped)
    keep_counts = Counter(r["algo_type"] for r in kept)
    print(f"\n── Фильтр выбросов (IQR ×{IQR_FACTOR}) ─────────────────────────────")
    print(f"  {'Алгоритм':<22} {'Оставлено':>10}  {'Удалено':>8}  {'Удалено, %':>10}")
    for algo in sorted(algos):
        k = keep_counts.get(algo, 0)
        d = drop_counts.get(algo, 0)
        pct = 100 * d / (k + d) if (k + d) else 0
        print(f"  {algo:<22} {k:>10}  {d:>8}  {pct:>9.1f}%")
    print(f"  {'ИТОГО':<22} {len(kept):>10}  {len(dropped):>8}  "
          f"{100*len(dropped)/len(records):.1f}%")

    # Диагностика: по какой метрике улетают прогоны
    print(f"\n  Причины удаления (первая сработавшая метрика):")
    print(f"  {'Алгоритм':<22} {'Метрика':<20} {'Удалено':>8}")
    print(f"  {'─'*52}")
    drop_by_algo_metric = Counter(
        (r["algo_type"], r["_drop_reason"]) for r in dropped
    )
    for (algo, metric), count in sorted(drop_by_algo_metric.items()):
        print(f"  {algo:<22} {metric:<20} {count:>8}")
    print(f"─────────────────────────────────────────────────────────────\n")

    # Убираем служебное поле
    for r in dropped:
        r.pop("_drop_reason", None)

    return kept


def sorted_algos(records):
    present = {r["algo_type"] for r in records}
    return [a for a in ALGO_ORDER if a in present] + sorted(present - set(ALGO_ORDER))


def algo_color(algo: str) -> str:
    return ALGO_COLORS.get(algo, "#999999")


# ── Общий boxplot ─────────────────────────────────────────────────────────────

def boxplot(records, field: str, title: str, yaxis_title: str,
            filename: str, output_dir: Path):
    algos = sorted_algos(records)
    fig = go.Figure()

    for algo in algos:
        values = [r[field] for r in records
                  if r["algo_type"] == algo and r[field] is not None]
        if not values:
            continue
        fig.add_trace(go.Box(
            y=values,
            name=algo,
            marker_color=algo_color(algo),
            boxmean="sd",
            hovertemplate=(
                f"<b>{algo}</b><br>"
                f"{title}: %{{y:.2f}}<br>"
                "Конфиг: %{text}<extra></extra>"
            ),
            text=[r["description"] for r in records
                  if r["algo_type"] == algo and r[field] is not None],
        ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),
        yaxis_title=yaxis_title,
        xaxis_title="Алгоритм",
        template="plotly_white",
        height=520,
        showlegend=False,
        boxmode="group",
    )

    out = output_dir / filename
    fig.write_html(str(out))
    print(f"  ✓ {out.name}")


# ── CDF ───────────────────────────────────────────────────────────────────────

def plot_cdf(records, output_dir: Path):
    """
    CDF количества попыток. CONSTANT_SIZE исключён (ломает масштаб).
    Агрегируем tries_dist по всем прогонам каждого алгоритма.
    """
    filtered = [r for r in records if True]
    algos = sorted_algos(filtered)

    agg: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for r in filtered:
        for tries, count in r["tries_dist"].items():
            agg[r["algo_type"]][tries] += count

    fig = go.Figure()
    for algo in algos:
        dist = agg[algo]
        if not dist:
            continue
        total = sum(dist.values())
        xs, ys, cumulative = [], [], 0
        for t in range(1, max(dist.keys()) + 1):
            cumulative += dist.get(t, 0)
            xs.append(t)
            ys.append(cumulative / total)

        fig.add_trace(go.Scatter(
            x=xs, y=ys,
            mode="lines+markers",
            name=algo,
            line=dict(color=algo_color(algo), width=2),
            marker=dict(size=7),
            hovertemplate=(
                f"<b>{algo}</b><br>"
                "Попыток ≤ %{x}: %{y:.1%}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=dict(
            text="CDF: доля задач, выполненных за ≤ N попыток",
            font=dict(size=18),
        ),
        xaxis=dict(title="Количество попыток", dtick=10),
        yaxis=dict(title="Доля задач", tickformat=".0%", range=[0, 1.05]),
        legend=dict(title="Алгоритм"),
        template="plotly_white",
        height=500,
    )

    out = output_dir / "cdf_tries.html"
    fig.write_html(str(out))
    print(f"  ✓ {out.name}")


# ── Heatmap ───────────────────────────────────────────────────────────────────

def plot_heatmap(records, metric: str, output_dir: Path):
    algos = sorted_algos(records)
    all_descs = sorted({r["description"] for r in records if r[metric] is not None})

    def short(desc: str) -> str:
        parts = desc.split("__")
        return "/".join(parts[-2:]) if len(parts) >= 2 else desc

    cell: dict[str, dict[str, float]] = {a: {} for a in algos}
    for r in records:
        if r[metric] is not None:
            cell[r["algo_type"]][r["description"]] = r[metric]

    higher_is_better = metric in ("avg_util",)
    desc_means = {}
    for desc in all_descs:
        vals = [cell[a].get(desc) for a in algos if cell[a].get(desc) is not None]
        desc_means[desc] = statistics.mean(vals) if vals else math.inf
    all_descs_sorted = sorted(
        all_descs,
        key=lambda d: desc_means[d],
        reverse=higher_is_better,
    )

    z = []
    text = []
    for algo in algos:
        row_z, row_t = [], []
        for desc in all_descs_sorted:
            v = cell[algo].get(desc)
            row_z.append(v)
            row_t.append(f"{v:.1f}" if v is not None else "—")
        z.append(row_z)
        text.append(row_t)

    metric_titles = {
        "total_time": "Время выполнения (вирт. сек)",
        "overload_count": "Количество перегрузок",
        "avg_util": "Средний utilization",
    }

    fig = go.Figure(go.Heatmap(
        z=z,
        x=[short(d) for d in all_descs_sorted],
        y=algos,
        text=text,
        texttemplate="%{text}",
        colorscale="RdYlGn",
        reversescale=not higher_is_better,
        hoverongaps=False,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Конфиг: %{x}<br>"
            "Значение: %{text}<extra></extra>"
        ),
        colorbar=dict(title=metric),
    ))

    fig.update_layout(
        title=dict(
            text=f"Heatmap: {metric_titles.get(metric, metric)} — алгоритм × конфигурация",
            font=dict(size=18),
        ),
        xaxis=dict(
            title="Конфигурация алгоритма",
            tickangle=-60,
            tickfont=dict(size=8),
            automargin=True,
        ),
        yaxis=dict(title="Алгоритм"),
        height=max(400, 80 * len(algos)),
        template="plotly_white",
        margin=dict(b=200),
    )

    out = output_dir / "heatmap.html"
    fig.write_html(str(out))
    print(f"  ✓ {out.name}")


# ── Сводная таблица метрик ────────────────────────────────────────────────────

def print_metrics_summary(records):
    algos = sorted_algos(records)

    metrics = [
        ("tries_mean", "Среднее попыток/задача"),
        ("tries_median", "Медиана попыток"),
        ("tries_max", "Макс. попыток"),
        ("first_try_rate", "Доля с 1-й попытки, %"),
        ("overload_count", "Перегрузок (шт.)"),
        ("avg_util", "Утилизация (0–1)"),
        ("total_time", "Вирт. время (сек)"),
    ]

    SEP = "─" * 110
    print("\n" + "═" * 110)
    print("  СВОДНАЯ ТАБЛИЦА МЕТРИК ПО АЛГОРИТМАМ")
    print("  Формат: median  [p25 – p75]  |  min / max  |  N прогонов")
    print("═" * 110)

    for metric_key, metric_label in metrics:
        print(f"\n  ▶ {metric_label}")
        print(SEP)
        header = f"  {'Алгоритм':<22} {'Median':>10}  {'[P25 – P75]':>20}  {'Min':>10}  {'Max':>10}  {'N':>5}"
        print(header)
        print(SEP)

        for algo in algos:
            vals = [r[metric_key] for r in records
                    if r["algo_type"] == algo and r.get(metric_key) is not None]
            if not vals:
                print(f"  {algo:<22} {'—':>10}  {'—':>20}  {'—':>10}  {'—':>10}  {'0':>5}")
                continue

            if metric_key == "first_try_rate":
                vals = [v * 100 for v in vals]

            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            median = statistics.median(vals_sorted)
            p25 = vals_sorted[max(0, int(n * 0.25))]
            p75 = vals_sorted[min(n - 1, int(n * 0.75))]
            vmin = vals_sorted[0]
            vmax = vals_sorted[-1]

            if metric_key in ("tries_mean", "avg_util", "total_time", "first_try_rate"):
                fmt = ".2f"
            else:
                fmt = ".0f"
            width = 10
            print(
                f"  {algo:<22} "
                f"{median:>{width}{fmt}}  "
                f"[{p25:{fmt}} – {p75:{fmt}}]  "
                f"{vmin:>{width}{fmt}}  "
                f"{vmax:>{width}{fmt}}  "
                f"{n:>5}"
            )

        print(SEP)

    print("\n" + "═" * 110)
    print("  РЕЙТИНГ ПО КЛЮЧЕВЫМ МЕТРИКАМ (меньше = лучше, кроме утилизации)")
    print("═" * 110)

    ranking_metrics = [
        ("tries_mean", False, 3.0),
        ("overload_count", False, 2.0),
        ("avg_util", True, 1.0),
        ("total_time", False, 1.0),
    ]

    algo_scores = {a: 0.0 for a in algos}
    total_weight = sum(w for _, _, w in ranking_metrics)

    for metric_key, higher_is_better, weight in ranking_metrics:
        algo_medians = {}
        for algo in algos:
            vals = [r[metric_key] for r in records
                    if r["algo_type"] == algo and r[metric_key] is not None]
            if vals:
                algo_medians[algo] = statistics.median(vals)

        if not algo_medians:
            continue

        vmin = min(algo_medians.values())
        vmax = max(algo_medians.values())
        rng = vmax - vmin if vmax != vmin else 1.0

        for algo, v in algo_medians.items():
            norm = (v - vmin) / rng
            if higher_is_better:
                norm = 1.0 - norm
            algo_scores[algo] += norm * weight

    ranked = sorted(algos, key=lambda a: algo_scores[a])

    print(f"\n  {'#':<4} {'Алгоритм':<22} {'Score (меньше = лучше)':>25}")
    print(SEP)
    for rank, algo in enumerate(ranked, 1):
        score = algo_scores[algo]
        bar = "█" * int((1 - score / total_weight) * 20) if total_weight else ""
        print(f"  {rank:<4} {algo:<22} {score:>8.3f} / {total_weight:.1f}  {bar}")
    print(SEP)
    print(f"\n  ★  Рекомендуемый алгоритм: {ranked[0]}")
    print("     (минимальные перегрузки и ретраи, взвешенный рейтинг)")
    print("═" * 110 + "\n")


def compute_metrics_via_yield():
    # Читаем, фильтруем и сразу вычисляем метрики — всё за один проход
    # Накапливаем в список, чтобы print_metrics_summary мог итерировать несколько раз
    records = []
    for run in load_runs_via_yield(INPUT_DIR):
        records.append(compute_metrics(run))

    print(f"Записей до фильтрации выбросов: {len(records)}")
    records = remove_outliers_iqr(records)
    print(f"Записей для анализа: {len(records)}")
    print_metrics_summary(records)


# ── Точка входа ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import plotly
    except ImportError:
        print("plotly не установлен: pip install plotly")
        exit(1)

    runs = list(load_runs_via_yield(INPUT_DIR))

    if not runs:
        print("Нет данных после фильтрации — проверьте INPUT_DIR и MAX_DURATION_SEC")
        exit(1)

    from collections import Counter
    algo_counts = Counter(r["params"]["task_batch_provider_params"]["type"] for r in runs)
    print("\nПрошло фильтр по алгоритмам:")
    for algo, cnt in sorted(algo_counts.items()):
        print(f"  {algo:20s}: {cnt}")

    records = [compute_metrics(r) for r in runs]
    records = remove_outliers_iqr(records)

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\nСтроим графики:")

    boxplot(records, field="avg_util",
            title="Утилизация обработчика по алгоритмам",
            yaxis_title="Средний utilization (0–1)",
            filename="utilization.html", output_dir=output_dir)

    boxplot(records, field="overload_count",
            title="Количество перегрузок по алгоритмам",
            yaxis_title="Перегрузок, шт.",
            filename="overloads.html", output_dir=output_dir)

    boxplot(records, field="total_time",
            title="Виртуальное время выполнения всех задач",
            yaxis_title="Виртуальные секунды",
            filename="total_time.html", output_dir=output_dir)

    boxplot(records, field="tries_mean",
            title="Среднее количество попыток на задачу",
            yaxis_title="Попыток / задача",
            filename="retries.html", output_dir=output_dir)

    plot_cdf(records, output_dir)

    print(f"\nВсе графики → {output_dir.resolve()}")
    print_metrics_summary(records)
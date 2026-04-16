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
from plotly.subplots import make_subplots

# ── Настройки ─────────────────────────────────────────────────────────────────

INPUT_DIR        = "../simulation_results"     # ← папка с сырыми JSON результатами симуляции
OUTPUT_DIR       = "./plots_single_system"
MAX_DURATION_SEC = 60.0            # фильтр: прогон завершился за N реальных секунд
HEATMAP_METRIC   = "total_time"    # метрика для heatmap: total_time / overload_count / avg_utilization

ALGO_COLORS = {
    "CONSTANT_SIZE":   "#636EFA",
    "AIMD":            "#EF553B",
    "MOVING_PID":      "#00CC96",
    "MOVING_PID_V2":   "#AB63FA",
    "GRADIENT_ASCENT": "#FFA15A",
    "ADAPTIVE_MODEL":  "#19D3F3",
}
ALGO_ORDER = ["CONSTANT_SIZE", "AIMD", "MOVING_PID", "MOVING_PID_V2",
              "GRADIENT_ASCENT", "ADAPTIVE_MODEL"]


# ── Загрузка и фильтрация ─────────────────────────────────────────────────────

def load_runs(input_dir: str):
    runs = []
    for path in sorted(Path(input_dir).glob("*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                if data['history'][-1]['completed'] != data['history'][-1]['total']:
                    raise ValueError("NOT COMPLETED")
                runs.append(data)
        except Exception as e:
            print(f"  [SKIP] {path.name}: {e}")
    print(f"Загружено файлов: {len(runs)}")
    return runs


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
    history       = run.get("history", [])
    params        = run["params"]
    sys_p         = params["system_params"]
    algo_p        = params["task_batch_provider_params"]
    tries_dist_raw = run.get("succeed_tasks_count_by_tries_count", {})
    tries_dist    = {int(k): v for k, v in tries_dist_raw.items()}

    algo_type    = algo_p["type"]
    description  = algo_p.get("description", "")
    batch_opt    = algo_p.get("batch_opt", 0)
    total_tasks  = sys_p.get("tasks_amount", 0)

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
    active  = [s for s in history if s.get("executionCount", 0) > 0
               or s.get("queuedCount", 0) > 0]
    avg_util = (statistics.mean(s["executionCount"] / max_cap for s in active)
                if active else None)

    # Попытки
    total_task_count = sum(tries_dist.values())
    total_tries      = sum(k * v for k, v in tries_dist.items())
    tries_mean = total_tries / total_task_count if total_task_count else None
    tries_max  = max(tries_dist.keys()) if tries_dist else None

    return {
        "run_name":       run.get("run_name", ""),
        "algo_type":      algo_type,
        "description":    description,
        "batch_opt":      batch_opt,
        "duration":       run.get("duration"),
        "all_done":       all_done,
        "total_time":     total_time,
        "overload_count": overload_count,
        "avg_util":       avg_util,
        "tries_mean":     tries_mean,
        "tries_max":      tries_max,
        "tries_dist":     tries_dist,
    }


def sorted_algos(records):
    present = {r["algo_type"] for r in records}
    return [a for a in ALGO_ORDER if a in present] + sorted(present - set(ALGO_ORDER))


def algo_color(algo: str) -> str:
    return ALGO_COLORS.get(algo, "#999999")


# ── Общий boxplot ─────────────────────────────────────────────────────────────

def boxplot(records, field: str, title: str, yaxis_title: str,
            filename: str, output_dir: Path):
    algos = sorted_algos(records)
    fig   = go.Figure()

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
    filtered = [r for r in records if r["algo_type"] != "CONSTANT_SIZE"]
    algos    = sorted_algos(filtered)

    agg: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for r in filtered:
        for tries, count in r["tries_dist"].items():
            agg[r["algo_type"]][tries] += count

    fig = go.Figure()
    for algo in algos:
        dist  = agg[algo]
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
            text="CDF: доля задач, выполненных за ≤ N попыток (без CONSTANT_SIZE)",
            font=dict(size=18),
        ),
        xaxis=dict(title="Количество попыток", dtick=1),
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
    """
    Строки = алгоритмы, столбцы = description конфига алгоритма.
    Значение = метрика прогона (один прогон = одна ячейка).
    Если конфигов много — столбцы сортируются по медиане метрики.
    """
    algos = sorted_algos(records)

    # Собираем все уникальные descriptions
    all_descs = sorted({r["description"] for r in records if r[metric] is not None})

    # Для читаемости: оставляем только часть имени после последнего __
    def short(desc: str) -> str:
        parts = desc.split("__")
        # "aimd__opt_batch__var03" → "opt_batch/var03"
        return "/".join(parts[-2:]) if len(parts) >= 2 else desc

    # Строим матрицу: z[algo][desc] = значение (или None)
    cell: dict[str, dict[str, float]] = {a: {} for a in algos}
    for r in records:
        if r[metric] is not None:
            cell[r["algo_type"]][r["description"]] = r[metric]

    # Сортируем столбцы по среднему значению метрики (лучшие левее)
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

    z    = []
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
        "total_time":     "Время выполнения (вирт. сек)",
        "overload_count": "Количество перегрузок",
        "avg_util":       "Средний utilization",
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


# ── Точка входа ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import plotly
    except ImportError:
        print("plotly не установлен: pip install plotly")
        exit(1)

    runs = load_runs(INPUT_DIR)

    # Фильтрация
    passed = [r for r in runs if passes_filter(r)]
    dropped = len(runs) - len(passed)
    print(f"После фильтрации: {len(passed)} / {len(runs)} "
          f"(отброшено {dropped})")

    if not passed:
        print("Нет данных после фильтрации — проверьте INPUT_DIR и MAX_DURATION_SEC")
        exit(1)

    # Статистика по алгоритмам
    from collections import Counter
    algo_counts = Counter(r["params"]["task_batch_provider_params"]["type"]
                          for r in passed)
    print("\nПрошло фильтр по алгоритмам:")
    for algo, cnt in sorted(algo_counts.items()):
        print(f"  {algo:20s}: {cnt}")

    # Вычисляем метрики
    records = [compute_metrics(r) for r in passed]

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\nСтроим графики:")

    boxplot(records,
            field="avg_util",
            title="Утилизация обработчика по алгоритмам",
            yaxis_title="Средний utilization (0–1)",
            filename="utilization.html",
            output_dir=output_dir)

    boxplot(records,
            field="overload_count",
            title="Количество перегрузок по алгоритмам",
            yaxis_title="Перегрузок, шт.",
            filename="overloads.html",
            output_dir=output_dir)

    boxplot(records,
            field="total_time",
            title="Виртуальное время выполнения всех задач",
            yaxis_title="Виртуальные секунды",
            filename="total_time.html",
            output_dir=output_dir)

    boxplot(records,
            field="tries_mean",
            title="Среднее количество попыток на задачу",
            yaxis_title="Попыток / задача",
            filename="retries.html",
            output_dir=output_dir)

    plot_cdf(records, output_dir)
    plot_heatmap(records, metric=HEATMAP_METRIC, output_dir=output_dir)

    print(f"\nВсе графики → {output_dir.resolve()}")
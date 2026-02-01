import re
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

# Чтение лога из файла
with open('pid_log', encoding='utf-8') as f:
    log_text = f.read()

lines = log_text.strip().splitlines()

# Списки данных
times = []
batch_sizes = []

# Базовое время (условное, т.к. в логах нет реальных timestamp'ов)
current_time = datetime(2026, 1, 19, 0, 0, 0)

pattern_batch = r'batch_size=np\.float64\(([\d\.e+-]+)\)'

i = 0
while i < len(lines):
    line = lines[i].strip()

    # Добавляем ~0.15 сек на каждую строку (можно подстроить)
    current_time += timedelta(seconds=0.15)
    t = current_time

    m_batch = re.search(pattern_batch, line)
    if m_batch:
        try:
            size = float(m_batch.group(1))
            batch_sizes.append(size)
        except:
            pass

    i += 1

EXECUTION_PERIOD = 50
points = len(batch_sizes) / EXECUTION_PERIOD
period = int(EXECUTION_PERIOD / points)
times = [current_time + timedelta(seconds=i * period) for i in range(len(batch_sizes))]
if not times or not batch_sizes:
    print("Не найдено ни одного batch_size в логе")
    exit()

# Преобразуем в pandas для удобного сглаживания
df = pd.DataFrame({'time': times, 'batch_size': batch_sizes})
df = df.set_index('time')


# ────────────────────────────────────────────────
# Один красивый график
# ────────────────────────────────────────────────

plt.figure(figsize=(14, 7))

# Основная кривая (точки + линия)
plt.plot(df.index, df['batch_size'], 'o-',
         color='royalblue', linewidth=1.2, markersize=4,
         alpha=0.6, label='batch_size')

# Отметка стабилизации (примерно, где cv обычно падает ниже 0.1)
# Предполагаем, что стабилизация начинается после ~30–40 итераций
stabilization_time = df.index[30] if len(df) > 30 else df.index[-1]
plt.axvline(x=stabilization_time, color='green', linestyle='--', alpha=0.7,
            label='пример начала стабилизации')

plt.title('Изменение размера батча во времени (PID-контроллер)', fontsize=16, pad=15)
plt.xlabel('Время (условное)', fontsize=12)
plt.ylabel('Размер батча', fontsize=12)

plt.grid(True, alpha=0.3, linestyle='--')
plt.legend(fontsize=11, loc='upper left')

# Форматирование оси X
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=12))
plt.xticks(rotation=35, ha='right')

plt.tight_layout()

# Показать график
plt.show()

# Сохранить в файл (раскомментируй если нужно)
# plt.savefig("batch_size_pid_single_plot.png", dpi=180, bbox_inches='tight')
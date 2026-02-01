import re
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


with open('aimd_log', encoding='utf-8') as f:
    log_lines = f.readlines()

# Регулярное выражение для извлечения времени и batch_size
pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*self\.current_batch_size=([\d\.e+-]+)'

times = []
sizes = []

for line in log_lines:
    match = re.search(pattern, line)
    if match:
        timestamp_str, size_str = match.groups()
        try:
            # Парсим время (формат с миллисекундами)
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            size = float(size_str)

            times.append(dt)
            sizes.append(size)
        except:
            pass  # пропускаем некорректные строки

# Если данных нет — ошибка
if not times:
    print("Не удалось найти ни одной строки с batch_size")
    exit()

# Строим график
plt.figure(figsize=(12, 6))
plt.plot(times, sizes, marker='o', markersize=3, linestyle='-', linewidth=1.2, color='#1f77b4')

# Форматирование оси X (время)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=15))
plt.xticks(rotation=45)

plt.title('Изменение batch_size во времени (AIMD)', fontsize=14)
plt.xlabel('Время', fontsize=12)
plt.ylabel('Размер батча', fontsize=12)
plt.grid(True, alpha=0.3, linestyle='--')

# Добавляем подписи progress (опционально)
progress_points = [line for line in log_lines if 'progress:' in line]
for prog_line in progress_points:
    try:
        ts_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', prog_line)
        if ts_match:
            ts = datetime.strptime(ts_match.group(1), '%Y-%m-%d %H:%M:%S,%f')
            plt.axvline(x=ts, color='gray', linestyle='--', alpha=0.4, linewidth=0.8)
            plt.text(ts, max(sizes) * 0.95, '50%/75%/90%', rotation=90, va='top', fontsize=9, alpha=0.7)
    except:
        pass

plt.tight_layout()
plt.show()

# Альтернатива: сохранить в файл
# plt.savefig('batch_size_over_time.png', dpi=150)
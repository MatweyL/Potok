"""
Адаптивный контроллер размера батчей с двухуровневым управлением
==================================================================

Архитектура:
- Уровень 1 (Тактический): PID-контроллер для быстрой стабилизации
- Уровень 2 (Стратегический): Адаптация границ на основе долгосрочных трендов

Автор: Система управления распределенными задачами
"""

import numpy as np
import time
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json


class ControllerPhase(Enum):
    """Фазы работы контроллера"""
    COLD_START = "cold_start"  # Экспоненциальное зондирование
    CALIBRATION = "calibration"  # Калибровка начальных границ
    OPERATIONAL = "operational"  # Основной режим работы


@dataclass
class SystemMetrics:
    """Метрики системы для принятия решений"""
    queue_depth: int  # Текущая глубина очереди
    queue_capacity: int  # Максимальная емкость очереди
    throughput: float  # Успешных задач/сек
    error_rate: float  # Доля ошибочных задач [0,1]
    avg_latency: float  # Среднее время выполнения задачи (сек)
    success_count: int = 0  # Количество успешных задач
    error_count: int = 0  # Количество ошибочных задач
    timestamp: float = field(default_factory=time.time)


@dataclass
class ControllerState:
    """Состояние контроллера для логирования и анализа"""
    phase: ControllerPhase
    batch_size: int
    Bmin: int
    Bmax: int
    utilization: float
    pid_error: float
    pid_signal: float
    quality_metric: float
    is_stable: bool
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            'phase': self.phase.value,
            'batch_size': self.batch_size,
            'Bmin': self.Bmin,
            'Bmax': self.Bmax,
            'utilization': round(self.utilization, 3),
            'pid_error': round(self.pid_error, 3),
            'pid_signal': round(self.pid_signal, 3),
            'quality_metric': round(self.quality_metric, 3),
            'is_stable': self.is_stable,
            'timestamp': self.timestamp
        }


class ColdStartProber:
    """Фаза экспоненциального зондирования для безопасного старта"""

    def __init__(self,
                 initial_batch: int = 10,
                 max_error_rate: float = 0.2,
                 max_latency_multiplier: float = 2.0):
        self.n = initial_batch
        self.max_error_rate = max_error_rate
        self.max_latency_multiplier = max_latency_multiplier
        self.baseline_latency: Optional[float] = None
        self.calibrated = False
        self.optimal_range: Optional[Tuple[int, int]] = None

    def next_batch_size(self, metrics: SystemMetrics) -> int:
        """Определяет следующий размер батча в фазе зондирования"""
        if self.calibrated:
            return self.optimal_range[1]  # Возвращаем верхнюю границу

        # Первая итерация - устанавливаем baseline
        if self.baseline_latency is None:
            self.baseline_latency = metrics.avg_latency
            return self.n

        # Проверка условий успеха
        latency_ok = metrics.avg_latency < self.baseline_latency * self.max_latency_multiplier
        errors_ok = metrics.error_rate < self.max_error_rate

        if latency_ok and errors_ok and metrics.success_count > 0:
            # Условия выполнены - удваиваем батч
            self.n *= 2
            return self.n
        else:
            # Достигли предела - калибруемся
            self.optimal_range = (max(10, self.n // 4), self.n // 2)
            self.calibrated = True
            return self.optimal_range[1]

    def is_ready(self) -> bool:
        return self.calibrated


class TacticalPIDController:
    """
    PID-контроллер для тактического управления размером батча
    Цель: удержание утилизации очереди на заданном уровне
    """

    def __init__(self,
                 Kp: float = 0.5,
                 Ki: float = 0.1,
                 Kd: float = 0.2,
                 target_utilization: float = 0.75,
                 anti_windup_limit: float = 1.0):
        # Коэффициенты PID
        self.Kp = Kp
        self.Ki = Ki
        self.Kd = Kd
        self.target = target_utilization
        self.anti_windup_limit = anti_windup_limit

        # Состояние контроллера
        self.integral = 0.0
        self.prev_error = 0.0

        # Границы управления
        self.Bmin = 100
        self.Bmax = 1000
        self.Bbase = (self.Bmin + self.Bmax) / 2

    def set_boundaries(self, Bmin: int, Bmax: int):
        """Установка границ управления (вызывается стратегическим уровнем)"""
        self.Bmin = max(10, Bmin)
        self.Bmax = max(self.Bmin + 10, Bmax)
        self.Bbase = (self.Bmin + self.Bmax) / 2

    def compute(self, current_utilization: float, dt: float) -> Tuple[int, Dict]:
        """
        Вычисление управляющего сигнала PID

        Returns:
            batch_size: Размер следующего батча
            info: Диагностическая информация
        """
        # Расчет ошибки
        error = self.target - current_utilization

        # Интегральная составляющая с anti-windup
        self.integral += error * dt
        self.integral = np.clip(self.integral,
                                -self.anti_windup_limit,
                                self.anti_windup_limit)

        # Дифференциальная составляющая
        derivative = (error - self.prev_error) / dt if dt > 0 else 0.0
        self.prev_error = error

        # Управляющий сигнал
        u = self.Kp * error + self.Ki * self.integral + self.Kd * derivative

        # Ограничение для плавного управления
        u = np.clip(u, -0.5, 0.5)

        # Расчет batch_size
        batch_size = self.Bbase * (1 + u)
        batch_size = int(np.clip(batch_size, self.Bmin, self.Bmax))

        # Проверка насыщения
        saturated = (batch_size == self.Bmin) or (batch_size == self.Bmax)

        return batch_size, {
            'error': error,
            'u': u,
            'integral': self.integral,
            'derivative': derivative,
            'saturated': saturated,
            'saturation_type': 'max' if batch_size == self.Bmax else 'min' if batch_size == self.Bmin else None
        }

    def reset(self):
        """Сброс интегральной составляющей (при резких изменениях)"""
        self.integral = 0.0
        self.prev_error = 0.0


class StrategicBoundaryAdapter:
    """
    Стратегический адаптер границ [Bmin, Bmax]
    Анализирует долгосрочные тренды и корректирует диапазон работы PID
    """

    def __init__(self,
                 adaptation_period: int = 10,
                 stability_threshold: float = 0.1,
                 error_rate_threshold: float = 0.2):
        self.period = adaptation_period
        self.stability_threshold = stability_threshold
        self.error_rate_threshold = error_rate_threshold

        # История для анализа трендов
        self.throughput_history: List[float] = []
        self.error_rate_history: List[float] = []
        self.saturation_history: List[bool] = []
        self.saturation_type_history: List[Optional[str]] = []

        self.batch_counter = 0
        self.pid_controller: Optional[TacticalPIDController] = None

    def update(self,
               throughput: float,
               error_rate: float,
               pid_saturated: bool,
               saturation_type: Optional[str]):
        """Обновление истории метрик"""
        self.throughput_history.append(throughput)
        self.error_rate_history.append(error_rate)
        self.saturation_history.append(pid_saturated)
        self.saturation_type_history.append(saturation_type)

        self.batch_counter += 1

        # Каждые N батчей проверяем необходимость адаптации
        if self.batch_counter >= self.period:
            self._adapt_boundaries()
            self.batch_counter = 0

    def _adapt_boundaries(self):
        """Адаптация границ на основе анализа истории"""
        if self.pid_controller is None:
            return

        window = min(self.period, len(self.throughput_history))
        if window < 3:  # Недостаточно данных
            return

        recent_throughput = self.throughput_history[-window:]
        recent_errors = self.error_rate_history[-window:]
        recent_saturations = self.saturation_history[-window:]
        recent_sat_types = self.saturation_type_history[-window:]

        # Метрики для принятия решений
        throughput_mean = np.mean(recent_throughput)
        throughput_std = np.std(recent_throughput)
        throughput_cv = throughput_std / throughput_mean if throughput_mean > 0 else 0

        error_mean = np.mean(recent_errors)
        saturation_rate = sum(recent_saturations) / window
        max_saturation_count = sum(1 for st in recent_sat_types if st == 'max')

        is_stable = throughput_cv < self.stability_threshold

        # Тренды (линейная регрессия)
        if window >= 3:
            error_trend = np.polyfit(range(window), recent_errors, 1)[0]
            throughput_trend = np.polyfit(range(window), recent_throughput, 1)[0]
        else:
            error_trend = 0
            throughput_trend = 0

        pid = self.pid_controller
        current_Bmax = pid.Bmax
        current_Bmin = pid.Bmin

        # === ПРАВИЛА АДАПТАЦИИ ===

        # ПРАВИЛО 1: Увеличение Bmax (рост емкости)
        # Условия: стабильность + постоянное насыщение на максе + низкий уровень ошибок
        if (is_stable and
                max_saturation_count > window * 0.7 and
                error_mean < self.error_rate_threshold):
            new_Bmax = int(current_Bmax * 1.05)
            pid.set_boundaries(current_Bmin, new_Bmax)
            print(f"📈 [STRATEGIC] Увеличение Bmax: {current_Bmax} → {new_Bmax}")
            print(
                f"   Причина: стабильность={is_stable}, насыщение={max_saturation_count}/{window}, ошибки={error_mean:.2%}")
            self._reset_history()
            return

        # ПРАВИЛО 2: Снижение Bmax (деградация системы)
        # Условия: рост ошибок + падение throughput
        if error_trend > 0.01 and throughput_trend < 0:
            new_Bmax = int(current_Bmax * 0.9)
            pid.set_boundaries(current_Bmin, new_Bmax)
            pid.reset()  # Сброс интегральной составляющей PID
            print(f"📉 [STRATEGIC] Снижение Bmax: {current_Bmax} → {new_Bmax}")
            print(f"   Причина: деградация (error_trend={error_trend:.4f}, throughput_trend={throughput_trend:.2f})")
            self._reset_history()
            return

        # ПРАВИЛО 3: Сжатие диапазона (недоиспользование)
        # Условия: постоянное насыщение на минимуме
        min_saturation_count = sum(1 for st in recent_sat_types if st == 'min')
        if min_saturation_count > window * 0.7:
            new_Bmax = int(current_Bmax * 0.95)
            new_Bmin = int(current_Bmin * 0.9)
            pid.set_boundaries(new_Bmin, new_Bmax)
            print(f"📊 [STRATEGIC] Сжатие диапазона: [{current_Bmin}, {current_Bmax}] → [{new_Bmin}, {new_Bmax}]")
            print(f"   Причина: недоиспользование (насыщение на минимуме {min_saturation_count}/{window})")
            self._reset_history()
            return

        # ПРАВИЛО 4: Экстренное снижение при критическом уровне ошибок
        if error_mean > 0.5:  # Более 50% ошибок - критично
            new_Bmax = int(current_Bmax * 0.7)
            new_Bmin = int(current_Bmin * 0.8)
            pid.set_boundaries(new_Bmin, new_Bmax)
            pid.reset()
            print(f"🚨 [STRATEGIC] ЭКСТРЕННОЕ снижение: [{current_Bmin}, {current_Bmax}] → [{new_Bmin}, {new_Bmax}]")
            print(f"   Причина: критический уровень ошибок {error_mean:.2%}")
            self._reset_history()
            return

    def _reset_history(self):
        """Сброс истории после адаптации"""
        # Оставляем последние 2 точки для непрерывности
        if len(self.throughput_history) > 2:
            self.throughput_history = self.throughput_history[-2:]
            self.error_rate_history = self.error_rate_history[-2:]
            self.saturation_history = self.saturation_history[-2:]
            self.saturation_type_history = self.saturation_type_history[-2:]
        self.batch_counter = 0


class AdaptiveBatchController:
    """
    Главный контроллер с двухуровневым управлением
    """

    def __init__(self,
                 target_utilization: float = 0.75,
                 pid_params: Optional[Dict] = None,
                 strategic_params: Optional[Dict] = None):

        # Инициализация фаз
        self.phase = ControllerPhase.COLD_START
        self.cold_start = ColdStartProber()

        # Тактический уровень (PID)
        pid_config = pid_params or {}
        self.tactical = TacticalPIDController(
            Kp=pid_config.get('Kp', 0.5),
            Ki=pid_config.get('Ki', 0.1),
            Kd=pid_config.get('Kd', 0.2),
            target_utilization=target_utilization
        )

        # Стратегический уровень
        strategic_config = strategic_params or {}
        self.strategic = StrategicBoundaryAdapter(
            adaptation_period=strategic_config.get('period', 10),
            stability_threshold=strategic_config.get('stability', 0.1),
            error_rate_threshold=strategic_config.get('error_threshold', 0.2)
        )
        self.strategic.pid_controller = self.tactical

        # Состояние
        self.last_update_time = time.time()
        self.iteration_count = 0
        self.state_history: List[ControllerState] = []

    def get_next_batch_size(self, metrics: SystemMetrics) -> Tuple[int, ControllerState]:
        """
        Основной метод: получение размера следующего батча

        Args:
            metrics: Текущие метрики системы

        Returns:
            batch_size: Размер следующего батча
            state: Состояние контроллера для мониторинга
        """
        current_time = time.time()
        dt = current_time - self.last_update_time
        self.last_update_time = current_time
        self.iteration_count += 1

        # === ФАЗА 1: ХОЛОДНЫЙ СТАРТ ===
        if self.phase == ControllerPhase.COLD_START:
            batch_size = self.cold_start.next_batch_size(metrics)

            if self.cold_start.is_ready():
                # Переход к калибровке
                self.phase = ControllerPhase.CALIBRATION
                Bmin, Bmax = self.cold_start.optimal_range
                self.tactical.set_boundaries(Bmin, Bmax)
                print(f"✅ [COLD START] Завершен. Оптимальный диапазон: [{Bmin}, {Bmax}]")

            state = ControllerState(
                phase=self.phase,
                batch_size=batch_size,
                Bmin=0,
                Bmax=0,
                utilization=0.0,
                pid_error=0.0,
                pid_signal=0.0,
                quality_metric=0.0,
                is_stable=False
            )
            self.state_history.append(state)
            return batch_size, state

        # === ФАЗА 2 и 3: КАЛИБРОВКА И ОСНОВНОЙ РЕЖИМ ===

        # Вычисление утилизации
        utilization = metrics.queue_depth / metrics.queue_capacity if metrics.queue_capacity > 0 else 0

        # Тактический уровень: PID
        batch_size, pid_info = self.tactical.compute(utilization, dt)

        # Переход к основному режиму после нескольких итераций калибровки
        if self.phase == ControllerPhase.CALIBRATION and self.iteration_count > 5:
            self.phase = ControllerPhase.OPERATIONAL
            print(f"✅ [CALIBRATION] Завершена. Переход к основному режиму")

        # Стратегический уровень: адаптация границ (только в основном режиме)
        if self.phase == ControllerPhase.OPERATIONAL:
            self.strategic.update(
                throughput=metrics.throughput,
                error_rate=metrics.error_rate,
                pid_saturated=pid_info['saturated'],
                saturation_type=pid_info['saturation_type']
            )

        # Вычисление метрики качества системы
        quality = self._compute_quality_metric(metrics)

        # Проверка стабильности (на основе последних N итераций)
        is_stable = self._check_stability()

        state = ControllerState(
            phase=self.phase,
            batch_size=batch_size,
            Bmin=self.tactical.Bmin,
            Bmax=self.tactical.Bmax,
            utilization=utilization,
            pid_error=pid_info['error'],
            pid_signal=pid_info['u'],
            quality_metric=quality,
            is_stable=is_stable
        )

        self.state_history.append(state)

        # Ограничение истории (храним последние 100 состояний)
        if len(self.state_history) > 100:
            self.state_history = self.state_history[-100:]

        return batch_size, state

    def _compute_quality_metric(self, metrics: SystemMetrics) -> float:
        """
        Вычисление комплексной метрики качества системы

        Q = 0.4·throughput_norm + 0.3·success_norm + 0.2·latency_norm + 0.1·queue_norm
        """
        # Нормализация компонентов
        max_throughput = max([s.quality_metric for s in self.state_history[-10:]], default=1.0)
        throughput_norm = min(metrics.throughput / max(max_throughput, 1.0), 1.0)

        success_norm = 1.0 - metrics.error_rate

        # Латентность (инвертированная нормализация)
        baseline_latency = 1.0  # условный baseline
        latency_norm = max(0, 1 - metrics.avg_latency / (baseline_latency * 3))

        # Очередь
        queue_norm = 1 - min(metrics.queue_depth / metrics.queue_capacity, 1.0)

        # Взвешенная сумма
        quality = (
                0.4 * throughput_norm +
                0.3 * success_norm +
                0.2 * latency_norm +
                0.1 * queue_norm
        )

        # Штраф за высокий уровень ошибок
        if metrics.error_rate > 0.2:
            quality *= ((1.0 - metrics.error_rate) / 0.8) ** 2

        return max(0.0, min(1.0, quality))

    def _check_stability(self, window: int = 10) -> bool:
        """Проверка стабильности на основе коэффициента вариации"""
        if len(self.state_history) < window:
            return False

        recent_quality = [s.quality_metric for s in self.state_history[-window:]]
        mean_quality = np.mean(recent_quality)
        std_quality = np.std(recent_quality)

        if mean_quality == 0:
            return False

        cv = std_quality / mean_quality
        return cv < 0.1  # CV < 10% считаем стабильным

    def get_diagnostics(self) -> Dict:
        """Получение диагностической информации"""
        if not self.state_history:
            return {}

        recent_states = self.state_history[-10:]

        return {
            'phase': self.phase.value,
            'iteration': self.iteration_count,
            'current_batch_size': recent_states[-1].batch_size,
            'boundaries': {
                'Bmin': self.tactical.Bmin,
                'Bmax': self.tactical.Bmax,
                'Bbase': self.tactical.Bbase
            },
            'pid': {
                'integral': self.tactical.integral,
                'prev_error': self.tactical.prev_error,
                'target_utilization': self.tactical.target
            },
            'recent_quality': [s.quality_metric for s in recent_states],
            'recent_utilization': [s.utilization for s in recent_states],
            'is_stable': recent_states[-1].is_stable
        }

    def export_history(self, filepath: str):
        """Экспорт истории состояний в JSON"""
        history_data = [state.to_dict() for state in self.state_history]
        with open(filepath, 'w') as f:
            json.dump(history_data, f, indent=2)
        print(f"📁 История экспортирована в {filepath}")


# ============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# ============================================================================

if __name__ == "__main__":
    # Инициализация контроллера
    controller = AdaptiveBatchController(
        target_utilization=0.75,
        pid_params={'Kp': 0.5, 'Ki': 0.1, 'Kd': 0.2},
        strategic_params={'period': 10, 'stability': 0.1}
    )

    print("=" * 60)
    print("ADAPTIVE BATCH CONTROLLER - DEMO")
    print("=" * 60)

    # Симуляция работы системы
    for iteration in range(50):
        # Симуляция метрик (в реальной системе берутся из мониторинга)
        metrics = SystemMetrics(
            queue_depth=int(500 + 200 * np.sin(iteration / 10)),
            queue_capacity=1000,
            throughput=50 + 10 * np.random.randn(),
            error_rate=max(0, min(0.3, 0.1 + 0.05 * np.random.randn())),
            avg_latency=2.0 + 0.5 * np.random.randn(),
            success_count=int(45 + 10 * np.random.randn()),
            error_count=int(5 + 2 * np.random.randn())
        )

        # Получение размера следующего батча
        batch_size, state = controller.get_next_batch_size(metrics)

        # Вывод информации каждые 5 итераций
        if iteration % 5 == 0:
            print(f"\n[Iter {iteration}] Phase: {state.phase.value}")
            print(f"  Batch size: {batch_size} (range: [{state.Bmin}, {state.Bmax}])")
            print(f"  Utilization: {state.utilization:.2%} (target: {controller.tactical.target:.2%})")
            print(f"  PID error: {state.pid_error:+.3f}, signal: {state.pid_signal:+.3f}")
            print(f"  Quality: {state.quality_metric:.3f}, Stable: {state.is_stable}")
            print(f"  Metrics: throughput={metrics.throughput:.1f}, errors={metrics.error_rate:.2%}")

        time.sleep(0.1)  # Симуляция времени между итерациями

    # Диагностика
    print("\n" + "=" * 60)
    print("FINAL DIAGNOSTICS")
    print("=" * 60)
    diag = controller.get_diagnostics()
    print(json.dumps(diag, indent=2))

    # Экспорт истории
    # controller.export_history('controller_history.json')
from typing import List

from imitation_modelling.handler import HandlerPool
from imitation_modelling.repo import TaskRunMetricProvider
from imitation_modelling.schemas import InputHandlerScalingRule, HandlerScalingRule, ScaleDirection


class HandlerScalingController:
    """
    Динамически меняет количество обработчиков в пуле в зависимости от прогресса.
    Правила применяются строго по порядку (от меньшего threshold к большему)
    и каждое правило срабатывает максимум один раз.
    """

    def __init__(
        self,
        rules: List[InputHandlerScalingRule],
        handler_pool: HandlerPool,
        metric_provider: TaskRunMetricProvider,
    ):
        # Сортируем один раз при создании + добавляем флаг executed
        self._rules = [
            HandlerScalingRule.model_validate(rule, from_attributes=True)
            for rule in sorted(rules, key=lambda r: r.threshold)
        ]
        self._handler_pool = handler_pool
        self._metric_provider = metric_provider

    def apply_scaling(self) -> None:          # было control()
        if not self._rules:
            return

        completed_ratio = (
            self._metric_provider.get_completed_count()
            / self._metric_provider.get_total_count()
        )

        for rule in self._rules:
            if rule.executed:
                continue

            if completed_ratio >= rule.threshold:
                if rule.direction == ScaleDirection.INCREASE:
                    self._handler_pool.increase(rule.amount)
                elif rule.direction == ScaleDirection.DECREASE:
                    self._handler_pool.terminate(rule.amount)
                else:
                    raise ValueError(f"Unknown scale direction: {rule.direction}")

                rule.executed = True
            break

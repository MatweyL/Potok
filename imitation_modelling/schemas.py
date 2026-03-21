import enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class TaskRunStatus(str, enum.Enum):
    WAITING = "WAITING"
    QUEUED = "QUEUED"
    EXECUTION = "EXECUTION"
    INTERRUPTED = "INTERRUPTED"
    TEMP_ERROR = "TEMP_ERROR"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    SUCCEED = "SUCCEED"


class SystemTime:

    def __init__(self, base_time_iso: str = "2025-10-12 12:00:00", time_step_seconds: float = 1.0, ):
        self._time_current = datetime.strptime(base_time_iso, "%Y-%m-%d %H:%M:%S")
        self._time_start = self._time_current
        self._time_step_seconds = timedelta(seconds=time_step_seconds)

    def tick(self):
        self._time_current += self._time_step_seconds

    @property
    def start(self):
        return self._time_start

    @property
    def current(self):
        return self._time_current


@dataclass
class TaskRun:
    id: str


@dataclass
class TaskRunStatusLog:
    """Единичное выполнение задачи"""
    task_run_id: str
    status: TaskRunStatus
    created_timestamp: datetime


@dataclass
class TaskExecution:
    task_run: TaskRun
    finish_time: datetime
    next_execution_confirm_time: datetime


class ScaleDirection(str, enum.Enum):
    """Направление масштабирования пула обработчиков"""
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"


class InputHandlerScalingRule(BaseModel):
    """Одно правило автоскейлинга по прогрессу задач"""
    direction: ScaleDirection
    threshold: float  # раньше complete_ratio
    amount: int


class HandlerScalingRule(InputHandlerScalingRule):
    executed: bool = False


class TaskBatchProviderType(str, enum.Enum):
    CONSTANT_SIZE = "CONSTANT_SIZE"
    AIMD = "AIMD"
    MOVING_PID = "MOVING_PID"
    MOVING_PID_V2 = "MOVING_PID_V2"
    GRADIENT_ASCENT = "GRADIENT_ASCENT"
    ADAPTIVE_MODEL = "ADAPTIVE_MODEL"


class SystemParams(BaseModel):
    handlers_amount: int = Field(default=5, description="Количество обработчиков")
    handler_max_tasks: int = Field(default=4, description="Максимальное количество задач,"
                                                          " которые обработчик может выполнять одновременно")
    execution_confirm_timeout: int = Field(default=300, description="Подтверждать выполнение задачи каждые"
                                                                    " N виртуальных секунд."
                                                                    " Значение обязательно должно быть меньше, чем"
                                                                    " таймаут перевода задачи в статус прерванной")
    tasks_part_from_all_for_high_load: float = Field(default=0.9,
                                                     description="Загруженность обработчика, при которой появляется "
                                                                 "вероятность отказа выполняющейся задачи. "
                                                                 "Загруженность вычисляется "
                                                                 "как tasks_in_work / handler_max_tasks")
    temp_error_probability_at_high_load: float = Field(default=0.1, description="Вероятность отказа для задачи"
                                                                                " в загруженном сборщике")
    random_timeout_generator_left: int = Field(default=10, description="Минимальное время выполнения задачи,"
                                                                       " виртуальные секунды")
    random_timeout_generator_right: int = Field(default=15, description="Максимальное время выполнения задачи,"
                                                                        " виртуальные секунды")
    tasks_amount: int = Field(default=1000, description="Количество задач в постановщике задач,"
                                                        " на которых проводится тест")
    interrupted_timeout: int = Field(default=400, description="Таймаут перевода задачи в статус прерванной для"
                                                              " постановщика задач, виртуальные секунды")
    run_timeout: int = Field(default=30, description="Период отправки задач на постановщиком задач в очередь,"
                                                     " виртуальные секунды")
    metric_provider_period: int = Field(default=150, description="Период, за который класс предоставления метрик"
                                                                 " считает частоты появления статусов,"
                                                                 " виртуальные секунды")
    time_step_seconds: int = Field(default=25, description="Количество виртуальных секунд, которые проходят за один"
                                                           " tick виртуальных часов")
    broker_task_ttl: int = Field(default=400, description="Время жизни задачи в очереди в виртуальных секундах."
                                                          " Значение обязательно должно"
                                                          " быть меньше, чем таймаут перевода"
                                                          " задачи в статус выполненной")
    handler_scaling_rules: List[InputHandlerScalingRule] = Field(default_factory=list,
                                                                 description="Правила для увеличения или уменьшения"
                                                                             " количества обработчиков в системе"
                                                                             " после выполнения определенной доли задач")
    config_name: str = Field(default='default', description="Название текущей конфигурации")

    max_run_seconds: int = Field(default=180,
                                 description="Максимальная длительность исследования в реальном мире"
                                             " в секундах. Для удобства сравнения результатов работы алгоритмов"
                                             " лучше всегда держать постоянным, например, 180 секунд в каждом запуске")


class TaskBatchProviderParams(BaseModel):
    arguments: Dict[str, Any] = Field(description="Параметры выбранного алгоритма формирования размера пачки задач")
    type: TaskBatchProviderType = Field(description="Тип выбранного алгоритма формирования размера пачки задач")

    description: str = Field(description="Краткий комментарий об используемой конфигурации")
    system_config_name: str = Field(description="Указание конфигурации симуляции, для которой применяется этот конфиг")
    batch_min: int = Field(description="Минимальный размер пачки задач")
    batch_opt: int = Field(description="Оптимальный размер пачки задач для симуляции")
    batch_max: int = Field(description="Максимальный размер пачки задач для симуляции")


class SimulationParams(BaseModel):
    system_params: SystemParams
    task_batch_provider_params: TaskBatchProviderParams

    @cached_property
    def run_name(self) -> str:
        return f"{self.task_batch_provider_params.type.value}__{self.task_batch_provider_params.description}__{self.system_params.config_name}"

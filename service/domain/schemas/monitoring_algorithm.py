from typing import List

from pydantic import BaseModel, Field

from service.domain.schemas.enums import MonitoringAlgorithmType


class MonitoringAlgorithmPK(BaseModel):
    id: int = None


class MonitoringAlgorithm(MonitoringAlgorithmPK):
    """ Для каждого алгоритма мониторинга свой провайдер задач, готовых к выполнению """
    type: MonitoringAlgorithmType


class PeriodicMonitoringAlgorithm(MonitoringAlgorithm):
    type: MonitoringAlgorithmType = MonitoringAlgorithmType.PERIODIC
    timeout: float = Field(description="Период, через который будет выполняться задача")
    timeout_noize: float = Field(default=0, description="Число большее нуля, которое случайно будет прибавляться или "
                                                        "отниматься от значения timeout при вычислении времени "
                                                        "следующего запуска задачи. ")


class SingleMonitoringAlgorithm(MonitoringAlgorithm):
    type: MonitoringAlgorithmType = MonitoringAlgorithmType.SINGLE
    timeouts: List[float] = Field(default_factory=list,
                                  description="Таймауты для сна после выполнения задачи. "
                                              "Длина списка минус один - сколько раз задача будет выполнена. "
                                              "Если длина равна нулю, то считается, что задача будет выполнена "
                                              "1 раз при первой возможности выполнения. ")
    timeout_noize: float = 0

from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


class TaskGroupPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, TaskGroupPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class TaskGroupBody(BaseModel):
    name: str = Field(description="Техническое название группы для формирования очереди в брокере")
    title: str = Field(description="Человеко-понятное название группы задач")
    description: str = Field(description="Описание группы задач")
    is_active: bool = Field(description="Является ли группа задач активной, будут ли из нее отправляться задачи"
                                        " на выполнение. Может быть полезно, если нужно быстро выключить постановку"
                                        " задач в определенный сборщик",
                            default=True)
    execution_arguments: Optional[Dict[str, Any]] = Field(default=None,
                                                          description="Аргументы выполнения, общие "
                                                                      "для всей группы задач. Передаются в поле "
                                                                      "execution_arguments каждой задачи. "
                                                                      "Если у задачи есть свои execution_arguments, "
                                                                      "то ее аргументы выполнения "
                                                                      "расширяют аргументы выполнения группы задач")

    # consumer_engine_id: int = Field(description="Идентификатор объекта, принимающего результаты выполнения задач")
    # producer_engine_id: int = Field(description="Идентификатор объекта, отправляющий задачи на выполнение")
    queue_per_priority: bool = Field(description="Определяет стратегию создания очередей для всей группы задач."
                                                 "Если queue_per_priority = True, то для группы создаётся несколько"
                                                 " очередей (по одной на каждый приоритет)."
                                                 "Если False — то одна общая очередь для всех приоритетов группы",
                                     default=True)


class TaskGroup(TaskGroupPK, TaskGroupBody):
    pass


class TaskGroupStatistics(BaseModel):
    group_name: str = Field(description="Название группы задач, для которой вычислены метрики")
    period_s: int = Field(description="Период, за который вычислены метрики")

    total_count: int = Field(default=0, description="Всего задач в группе")
    error_count: Optional[int] = Field(default=0, description="Задач, не подлежащих повторным попыткам выполнения")
    queued_count: Optional[int] = Field(default=0, description="Задач в очереди")
    succeed_count: Optional[int] = Field(default=0, description="Выполненных задач")
    waiting_count: Optional[int] = Field(default=0, description="Ожидающих выполнения задач")
    execution_count: Optional[int] = Field(default=0, description="Выполняющихся задач")
    interrupted_count: Optional[int] = Field(default=0, description="Прерванных задач, которые будут выполнены "
                                                                    "повторно позже")
    temp_error_count: Optional[int] = Field(default=0, description="Задач, подлежащих повторным попыткам выполнения")
    cancelled_count: Optional[int] = Field(default=0,
                                           description="Отмененных задач, их выполнение можно возобновить")

    avg_queued_duration: float = Field(default=0, description="Среднее время задачи в очереди, сек")
    avg_execution_duration: float = Field(default=0, description="Среднее время выполнения задачи, сек")
    avg_retry_count: float = Field(default=0, description="Среднее количество повторных выполнений задачи. "
                                                          "Фактически, это количество раз появления "
                                                          "статуса WAITING у задачи ")
    throughput: float = Field(default=0, description="Пропускная способность системы, задач в секунду")
    completed_count: float = Field(default=0, description="Количество завершенных задач, succeed + error + cancelled")

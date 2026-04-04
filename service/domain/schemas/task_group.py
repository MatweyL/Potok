
from pydantic import BaseModel, Field


class TaskGroupPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, TaskGroupPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class TaskGroup(TaskGroupPK):
    name: str = Field(description="Техническое название группы для формирования очереди в брокере")
    title: str = Field(description="Человеко-понятное название группы задач")
    description: str = Field(description="Описание группы задач")
    is_active: bool = Field(description="Является ли группа задач активной, будут ли из нее отправляться задачи"
                                        " на выполнение. Может быть полезно, если нужно быстро выключить постановку"
                                        " задач в определенный сборщик",
                            default=True)

    # consumer_engine_id: int = Field(description="Идентификатор объекта, принимающего результаты выполнения задач")
    # producer_engine_id: int = Field(description="Идентификатор объекта, отправляющий задачи на выполнение")
    queue_per_priority: bool = Field(description="Определяет стратегию создания очередей для всей группы задач."
                                                 "Если queue_per_priority = True, то для группы создаётся несколько"
                                                 " очередей (по одной на каждый приоритет)."
                                                 "Если False — то одна общая очередь для всех приоритетов группы",
                                     default=True)

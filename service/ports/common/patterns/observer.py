from abc import ABC, abstractmethod
from typing import List

from service.ports.common.logs import logger


class Subject(ABC):
    """
    Интерфейс издателя объявляет набор методов для управлениями подписчиками.
    """

    @abstractmethod
    def attach(self, observer: 'Observer') -> None:
        """
        Присоединяет наблюдателя к издателю.
        """
        pass

    @abstractmethod
    def detach(self, observer: 'Observer') -> None:
        """
        Отсоединяет наблюдателя от издателя.
        """
        pass

    @abstractmethod
    def notify(self) -> None:
        """
        Уведомляет всех наблюдателей о событии.
        """
        pass


class AbstractSubject(Subject, ABC):
    """
    Издатель владеет некоторым важным состоянием и оповещает наблюдателей о его
    изменениях.
    """
    def __init__(self):
        self._observers: List['Observer'] = []

    def attach(self, observer: 'Observer') -> None:
        logger.debug("Subject: Attached an observer.")
        self._observers.append(observer)

    def detach(self, observer: 'Observer') -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        """
        Запуск обновления в каждом подписчике.
        """

        logger.debug(f"Subject: {self.__class__.__name__}. Notifying observers ({len(self._observers)})...")
        for observer in self._observers:
            observer.update(self)


class Observer(ABC):
    """
    Интерфейс Наблюдателя объявляет метод уведомления, который издатели
    используют для оповещения своих подписчиков.
    """

    @abstractmethod
    def update(self, subject: Subject) -> None:
        """
        Получить обновление от субъекта.
        """
        pass

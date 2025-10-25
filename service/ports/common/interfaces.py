from abc import ABC, abstractmethod


class Startable(ABC):

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

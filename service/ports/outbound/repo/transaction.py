from abc import ABC, abstractmethod
from typing import Self


class Transaction(ABC):
    """
    Объект-транзакция, который передаётся в репозитории.
    Один экземпляр — одна транзакция.
    """

    @abstractmethod
    async def begin(self) -> None:
        """Начинает транзакцию."""
        pass

    @abstractmethod
    async def commit(self) -> None:
        """Фиксирует изменения."""
        pass

    @abstractmethod
    async def rollback(self) -> None:
        """Откатывает изменения."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Освобождает ресурсы транзакции."""
        pass

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit с автоматическим rollback при ошибке."""
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
        await self.close()


class TransactionFactory(ABC):

    @abstractmethod
    def create(self) -> Transaction:
        pass

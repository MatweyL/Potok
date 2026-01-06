from sqlalchemy.ext.asyncio import AsyncSession

from service.adapters.outbound.repo.sa.database import Database
from service.ports.outbound.repo.transaction import Transaction, TransactionFactory


class SATransaction(Transaction):

    def __init__(self, session: AsyncSession):
        self.session = session

    async def begin(self) -> None:
        await self.session.begin()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()

    async def close(self) -> None:
        await self.session.close()


class SATransactionFactory(TransactionFactory):
    def __init__(self, database: Database):
        self._database = database

    def create(self) -> SATransaction:
        return SATransaction(self._database.session)

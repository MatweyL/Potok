from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession


class Database:

    def __init__(self, uri: str):
        self.engine = create_async_engine(uri, echo=False)
        self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)

    @property
    def session(self) -> AsyncSession:
        return self.sessionmaker()


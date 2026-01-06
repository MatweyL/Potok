from datetime import datetime
from typing import Optional, Dict

import pytest
import pytest_asyncio
from pydantic import BaseModel
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import mapped_column, Mapped

from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.base import Base, TablenameMixin
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.transaction import SATransaction
from service.ports.outbound.repo.fields import (
    FilterFieldsDNF,
    FilterFieldsConjunct,
    FilterField,
    ConditionOperation,
    PaginationQuery,
    UpdateFields, UpdateField
)


# === Test Models ===

class UserModel(Base, TablenameMixin):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class UserDomain(BaseModel):
    id: Optional[int] = None
    name: str
    email: str
    age: int
    created_at: Optional[datetime] = None


class UserPK(BaseModel):
    id: int

    def __eq__(self, other):
        return isinstance(other, UserPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


# === Test Repository ===

class UserRepo(AbstractSARepo):
    def __init__(self, database: Database):
        super().__init__(database, UserModel)

    def to_model(self, obj: UserDomain) -> UserModel:
        model = UserModel()
        if obj.id:
            model.id = obj.id
        model.name = obj.name
        model.email = obj.email
        model.age = obj.age
        if obj.created_at:
            model.created_at = obj.created_at
        return model

    def to_domain(self, obj: UserModel) -> UserDomain:
        return UserDomain(
            id=obj.id,
            name=obj.name,
            email=obj.email,
            age=obj.age,
            created_at=obj.created_at
        )

    def pk_to_model_pk(self, pk: UserPK) -> Dict:
        return {"id": pk.id}


# === Fixtures ===
@pytest_asyncio.fixture
async def sqlite_database():
    """Создаёт in-memory SQLite базу для тестов"""
    db = Database("sqlite+aiosqlite:///:memory:")

    # Создаём таблицы
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield db

    # Очистка
    await db.engine.dispose()


@pytest_asyncio.fixture
async def postgres_database():
    """Создаёт подключение к PostgreSQL и очищает данные после тестов"""
    # URL вашей тестовой базы данных
    db_url = "postgresql+asyncpg://postgres:onlyone@localhost:5432/test_db"
    db = Database(db_url)

    # Создаём таблицы
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield db

    # Очистка: удаляем все данные из таблиц
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await db.engine.dispose()


@pytest.fixture
def database(postgres_database):
    return postgres_database


@pytest.fixture
def user_repo(database):
    return UserRepo(database)


@pytest.fixture
def sample_user():
    return UserDomain(
        name="John Doe",
        email="john@example.com",
        age=30,
        created_at=datetime(2024, 1, 1)
    )


# === Integration Tests ===

class TestCreateIntegration:
    @pytest.mark.asyncio
    async def test_create_success(self, user_repo, sample_user):
        result = await user_repo.create(sample_user)

        assert result is not None
        assert result.id is not None
        assert result.name == "John Doe"
        assert result.email == "john@example.com"
        assert result.age == 30

    @pytest.mark.asyncio
    async def test_create_duplicate_email_conflict(self, user_repo):
        user1 = UserDomain(name="User1", email="same@test.com", age=25)
        user2 = UserDomain(name="User2", email="same@test.com", age=30)

        result1 = await user_repo.create(user1)
        result2 = await user_repo.create(user2)

        assert result1 is not None
        assert result2 is None  # Конфликт по unique email

    @pytest.mark.asyncio
    async def test_create_with_transaction_commit(self, user_repo, database):
        user = UserDomain(name="Trans User", email="trans@test.com", age=25)

        async with database.session as session:
            transaction = SATransaction(session)
            await transaction.begin()

            result = await user_repo.create(user, transaction=transaction)

            await transaction.commit()
            await transaction.close()

        # Проверяем что пользователь сохранился
        retrieved = await user_repo.get(UserPK(id=result.id))
        assert retrieved is not None
        assert retrieved.name == "Trans User"

    @pytest.mark.asyncio
    async def test_create_with_transaction_rollback(self, user_repo, database):
        user = UserDomain(name="Rollback User", email="rollback@test.com", age=25)

        async with database.session as session:
            transaction = SATransaction(session)
            await transaction.begin()

            result = await user_repo.create(user, transaction=transaction)
            created_id = result.id

            await transaction.rollback()
            await transaction.close()

        # Проверяем что пользователь НЕ сохранился
        retrieved = await user_repo.get(UserPK(id=created_id))
        assert retrieved is None


class TestCreateAllIntegration:
    @pytest.mark.asyncio
    async def test_create_all_success(self, user_repo):
        users = [
            UserDomain(name="User1", email="user1@test.com", age=25),
            UserDomain(name="User2", email="user2@test.com", age=30),
            UserDomain(name="User3", email="user3@test.com", age=35)
        ]

        result = await user_repo.create_all(users)

        assert len(result) == 3
        assert all(u.id is not None for u in result)
        assert result[0].name == "User1"
        assert result[2].name == "User3"

    @pytest.mark.asyncio
    async def test_create_all_empty_list(self, user_repo):
        result = await user_repo.create_all([])
        assert result == []

    @pytest.mark.asyncio
    async def test_create_all_with_conflict(self, user_repo):
        # Создаём первого пользователя
        await user_repo.create(UserDomain(name="Existing", email="exist@test.com", age=20))

        # Пытаемся создать batch с конфликтующим email
        users = [
            UserDomain(name="New1", email="new1@test.com", age=25),
            UserDomain(name="Conflict", email="exist@test.com", age=30),  # Конфликт
            UserDomain(name="New2", email="new2@test.com", age=35)
        ]

        result = await user_repo.create_all(users)

        # on_conflict_do_nothing должен пропустить конфликтующую запись
        assert len(result) == 2


class TestUpdateIntegration:
    @pytest.mark.asyncio
    async def test_update_success(self, user_repo):
        # Создаём пользователя
        user = await user_repo.create(
            UserDomain(name="Original", email="original@test.com", age=25)
        )

        # Обновляем
        fields = UpdateFields(group=[UpdateField(name="name", value="Updated"), UpdateField(name="age", value=35)])
        updated = await user_repo.update(UserPK(id=user.id), fields)

        assert updated is not None
        assert updated.name == "Updated"
        assert updated.age == 35
        assert updated.email == "original@test.com"  # Не изменился

    @pytest.mark.asyncio
    async def test_update_not_found(self, user_repo):
        fields = UpdateFields(group=[UpdateField(name="name", value="Ghost")])
        result = await user_repo.update(UserPK(id=99999), fields)

        assert result is None


class TestUpdateAllIntegration:
    @pytest.mark.asyncio
    async def test_update_all_success(self, user_repo):
        # Создаём пользователей
        users = await user_repo.create_all([
            UserDomain(name="User1", email="u1@test.com", age=20),
            UserDomain(name="User2", email="u2@test.com", age=25),
            UserDomain(name="User3", email="u3@test.com", age=30)
        ])

        # Обновляем первых двух
        fields_by_pk = {
            UserPK(id=users[0].id): UpdateFields(group=[UpdateField(name="age", value=21)]),
            UserPK(id=users[1].id): UpdateFields(group=[UpdateField(name="age", value=26)])
        }

        await user_repo.update_all(fields_by_pk)

        # Проверяем
        u1 = await user_repo.get(UserPK(id=users[0].id))
        u2 = await user_repo.get(UserPK(id=users[1].id))
        u3 = await user_repo.get(UserPK(id=users[2].id))

        assert u1.age == 21
        assert u2.age == 26
        assert u3.age == 30  # Не изменился


class TestGetIntegration:
    @pytest.mark.asyncio
    async def test_get_success(self, user_repo):
        created = await user_repo.create(
            UserDomain(name="John", email="john@test.com", age=30)
        )

        retrieved = await user_repo.get(UserPK(id=created.id))

        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.name == "John"

    @pytest.mark.asyncio
    async def test_get_not_found(self, user_repo):
        result = await user_repo.get(UserPK(id=99999))
        assert result is None


class TestGetAllIntegration:
    @pytest.mark.asyncio
    async def test_get_all_empty(self, user_repo):
        result = await user_repo.get_all()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_multiple(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="User1", email="u1@test.com", age=20),
            UserDomain(name="User2", email="u2@test.com", age=25),
            UserDomain(name="User3", email="u3@test.com", age=30)
        ])

        result = await user_repo.get_all()

        assert len(result) == 3
        names = {u.name for u in result}
        assert names == {"User1", "User2", "User3"}


class TestFilterIntegration:
    @pytest.mark.asyncio
    async def test_filter_eq(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Alice", email="alice@test.com", age=25),
            UserDomain(name="Bob", email="bob@test.com", age=30),
            UserDomain(name="Alice", email="alice2@test.com", age=35)
        ])

        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[FilterField(name="name", operation=ConditionOperation.EQ, value="Alice")]
                )
            ]
        )

        result = await user_repo.filter(filter_dnf)

        assert len(result) == 2
        assert all(u.name == "Alice" for u in result)

    @pytest.mark.asyncio
    async def test_filter_range(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Young", email="young@test.com", age=20),
            UserDomain(name="Middle", email="middle@test.com", age=30),
            UserDomain(name="Old", email="old@test.com", age=40)
        ])

        # Возраст >= 25 AND <= 35
        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[
                        FilterField(name="age", operation=ConditionOperation.GTE, value=25),
                        FilterField(name="age", operation=ConditionOperation.LTE, value=35)
                    ]
                )
            ]
        )

        result = await user_repo.filter(filter_dnf)

        assert len(result) == 1
        assert result[0].name == "Middle"

    @pytest.mark.asyncio
    async def test_filter_or_condition(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Alice", email="alice@test.com", age=25),
            UserDomain(name="Bob", email="bob@test.com", age=30),
            UserDomain(name="Charlie", email="charlie@test.com", age=35)
        ])

        # name = "Alice" OR age = 35
        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[FilterField(name="name", operation=ConditionOperation.EQ, value="Alice")]
                ),
                FilterFieldsConjunct(
                    group=[FilterField(name="age", operation=ConditionOperation.EQ, value=35)]
                )
            ]
        )

        result = await user_repo.filter(filter_dnf)

        assert len(result) == 2
        names = {u.name for u in result}
        assert names == {"Alice", "Charlie"}

    @pytest.mark.asyncio
    async def test_filter_is_null(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="HasDate", email="hasdate@test.com", age=25, created_at=datetime.now()),
            UserDomain(name="NoDate", email="nodate@test.com", age=30, created_at=None)
        ])

        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[FilterField(name="created_at", operation=ConditionOperation.IS_NULL, value=None)]
                )
            ]
        )

        result = await user_repo.filter(filter_dnf)

        assert len(result) == 1
        assert result[0].name == "NoDate"

    @pytest.mark.asyncio
    async def test_filter_not_null(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="HasDate", email="hasdate@test.com", age=25, created_at=datetime.now()),
            UserDomain(name="NoDate", email="nodate@test.com", age=30, created_at=None)
        ])

        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[FilterField(name="created_at", operation=ConditionOperation.NOT_NULL, value=None)]
                )
            ]
        )

        result = await user_repo.filter(filter_dnf)

        assert len(result) == 1
        assert result[0].name == "HasDate"


class TestPaginatedIntegration:
    @pytest.mark.asyncio
    async def test_paginated_limit_offset(self, user_repo):
        # Создаём 10 пользователей
        await user_repo.create_all([
            UserDomain(name=f"User{i}", email=f"user{i}@test.com", age=20 + i)
            for i in range(10)
        ])

        # Берём вторую страницу по 3 элемента
        pagination = PaginationQuery(
            limit_per_page=3,
            offset_page=3,
            order_by="age",
            asc_sort=True
        )

        result = await user_repo.paginated(pagination)

        assert len(result) == 3
        assert result[0].age == 23  # 4-й пользователь (offset=3)

    @pytest.mark.asyncio
    async def test_paginated_with_sorting(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Charlie", email="c@test.com", age=30),
            UserDomain(name="Alice", email="a@test.com", age=25),
            UserDomain(name="Bob", email="b@test.com", age=35)
        ])

        # Сортировка по возрасту по возрастанию
        pagination = PaginationQuery(order_by="age", asc_sort=True)
        result = await user_repo.paginated(pagination)

        assert len(result) == 3
        assert result[0].name == "Alice"  # age=25
        assert result[2].name == "Bob"  # age=35

        # Сортировка по убыванию
        pagination = PaginationQuery(order_by="age", asc_sort=False)
        result = await user_repo.paginated(pagination)

        assert result[0].name == "Bob"  # age=35
        assert result[2].name == "Alice"  # age=25

    @pytest.mark.asyncio
    async def test_paginated_with_filters(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Young1", email="y1@test.com", age=20),
            UserDomain(name="Middle1", email="m1@test.com", age=30),
            UserDomain(name="Middle2", email="m2@test.com", age=32),
            UserDomain(name="Old1", email="o1@test.com", age=40)
        ])

        pagination = PaginationQuery(
            filter_fields_dnf=FilterFieldsDNF(
                conjunctions=[
                    FilterFieldsConjunct(
                        group=[
                            FilterField(name="age", operation=ConditionOperation.GTE, value=25),
                            FilterField(name="age", operation=ConditionOperation.LT, value=35)
                        ]
                    )
                ]
            ),
            limit_per_page=10,
            order_by="age",
            asc_sort=True
        )

        result = await user_repo.paginated(pagination)

        assert len(result) == 2
        assert result[0].age == 30
        assert result[1].age == 32


class TestCountByFieldsIntegration:
    @pytest.mark.asyncio
    async def test_count_by_fields(self, user_repo):
        await user_repo.create_all([
            UserDomain(name="Young1", email="y1@test.com", age=20),
            UserDomain(name="Young2", email="y2@test.com", age=22),
            UserDomain(name="Old1", email="o1@test.com", age=40)
        ])

        filter_dnf = FilterFieldsDNF(
            conjunctions=[
                FilterFieldsConjunct(
                    group=[FilterField(name="age", operation=ConditionOperation.LT, value=30)]
                )
            ]
        )

        count = await user_repo.count_by_fields(filter_dnf)

        assert count == 2


class TestTransactionContextManager:
    @pytest.mark.asyncio
    async def test_transaction_auto_commit(self, user_repo, database):
        async with database.session as session:
            async with SATransaction(session) as transaction:
                await user_repo.create(
                    UserDomain(name="Auto", email="auto@test.com", age=25),
                    transaction=transaction
                )

        # Проверяем что данные сохранились
        users = await user_repo.get_all()
        assert len(users) == 1
        assert users[0].name == "Auto"

    @pytest.mark.asyncio
    async def test_transaction_auto_rollback_on_exception(self, user_repo, database):
        try:
            async with database.session as session:
                async with SATransaction(session) as transaction:
                    await user_repo.create(
                        UserDomain(name="Fail", email="fail@test.com", age=25),
                        transaction=transaction
                    )
                    raise ValueError("Test exception")
        except ValueError:
            pass

        # Проверяем что данные НЕ сохранились
        users = await user_repo.get_all()
        assert len(users) == 0


class TestConversionMethods:
    def test_to_model(self, user_repo):
        domain = UserDomain(
            id=1,
            name="Test",
            email="test@test.com",
            age=25,
            created_at=datetime(2024, 1, 1)
        )

        model = user_repo.to_model(domain)

        assert isinstance(model, UserModel)
        assert model.id == 1
        assert model.name == "Test"
        assert model.email == "test@test.com"
        assert model.age == 25
        assert model.created_at == datetime(2024, 1, 1)

    def test_to_domain(self, user_repo):
        model = UserModel()
        model.id = 1
        model.name = "Test"
        model.email = "test@test.com"
        model.age = 25
        model.created_at = datetime(2024, 1, 1)

        domain = user_repo.to_domain(model)

        assert isinstance(domain, UserDomain)
        assert domain.id == 1
        assert domain.name == "Test"
        assert domain.email == "test@test.com"
        assert domain.age == 25
        assert domain.created_at == datetime(2024, 1, 1)

    def test_pk_to_model_pk(self, user_repo):
        pk = UserPK(id=123)
        model_pk = user_repo.pk_to_model_pk(pk)

        assert model_pk == {"id": 123}
        assert isinstance(model_pk, dict)

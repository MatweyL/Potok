from uuid import uuid4

from sqlalchemy import MetaData, UUID, DateTime, func, BIGINT, inspect, Column, INT
from sqlalchemy.orm import mapped_column, Mapped, declared_attr, registry


def camel_to_snake(string: str) -> str:
    """
    Простая и понятная реализация.
    """
    if not string:
        return string

    result = []

    for i, char in enumerate(string):
        # Если символ заглавный
        if char.isupper():
            # Если не первый символ
            if i > 0:
                # Всегда добавляем подчеркивание перед заглавной
                result.append('_')

            # Добавляем символ в нижнем регистре
            result.append(char.lower())
        else:
            # Строчная буква - добавляем как есть
            result.append(char)

    # Теперь обрабатываем случаи с акронимами
    # Нам нужно объединить подряд идущие заглавные, разделенные подчеркиваниями
    intermediate = ''.join(result)

    # Ищем паттерны вида "_м_п_z" и заменяем их на "_mpz"
    # Делим строку по подчеркиваниям
    parts = intermediate.split('_')
    final_parts = []

    i = 0
    while i < len(parts):
        # Если текущая часть - одиночная строчная буква
        if len(parts[i]) == 1 and parts[i].islower():
            # Проверяем следующие части
            j = i
            single_letters = []
            while j < len(parts) and len(parts[j]) == 1 and parts[j].islower():
                single_letters.append(parts[j])
                j += 1

            if len(single_letters) >= 2:
                # Это акроним - объединяем буквы
                final_parts.append(''.join(single_letters))
                i = j
            else:
                # Одиночная буква
                final_parts.append(parts[i])
                i += 1
        else:
            # Обычная часть
            final_parts.append(parts[i])
            i += 1

    return '_'.join(filter(None, final_parts))  # filter(None) убирает пустые строки


naming_convention = {
    "all_column_names": lambda constraint, table: "_".join([column.name for column in constraint.columns.values()]),
    "ix": "ix_%(table_name)s_%(all_column_names)s",
    "uq": "uq_%(table_name)s_%(all_column_names)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(all_column_names)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)

_Base = mapper_registry.generate_base()


class Base(_Base, ):
    __abstract__ = True

    @classmethod
    @property
    def pk(cls) -> frozenset:
        """Возвращает набор имён первичных ключей"""
        # Кэшируем в атрибуте класса
        if not hasattr(cls, '_pk_cache'):
            cls._pk_cache = frozenset(
                column.name for column in inspect(cls).primary_key
            )
        return cls._pk_cache

    def to_dict(self):
        """Преобразует модель в словарь, исключает первичные ключи равные None """
        return {c.key: getattr(self, c.key)
                for c in inspect(self).mapper.column_attrs
                if not (c.key in self.pk and getattr(self, c.key) is None)}


class TablenameMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return camel_to_snake(cls.__name__)


class SerialBigIntPKMixin:
    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)


class SerialIntPKMixin:
    id: Mapped[int] = mapped_column(INT, primary_key=True, autoincrement=True)


class UUIDPKMixin:
    uid = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)


class LoadTimestampMixin:
    loaded_at = Column(DateTime, default=func.now)

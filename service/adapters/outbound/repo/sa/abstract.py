from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Type

from pydantic import BaseModel
from sqlalchemy import update, select, or_, ColumnElement, and_, asc, desc, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.operators import gt, eq, ge, lt, le

from service.adapters.outbound.repo.sa.base import Base
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.transaction import SATransaction
from service.ports.outbound.repo.abstract import Repo, TDomain, TPK
from service.ports.outbound.repo.fields import FilterFieldsDNF, PaginationQuery, UpdateFields, FilterFieldsConjunct, \
    FilterField, ConditionOperation


class AbstractSARepo(Repo, ABC):

    def __init__(self, database: Database, model_class: Type[Base]):
        self._database = database
        self._model_class = model_class

    @abstractmethod
    def to_model(self, obj: BaseModel) -> Base:
        """ Метод для конвертации объекта домена в модель хранения """
        pass

    @abstractmethod
    def to_domain(self, obj: Base) -> BaseModel:
        """ Метод для конвертации модели хранения в объект домена """
        pass

    @abstractmethod
    def pk_to_model_pk(self, pk: BaseModel) -> Dict:
        """ Метод для конвертации первичного ключа доменной модели в первичный ключ модели хранения """
        pass

    async def create(self,
                     obj: TDomain,
                     transaction: Optional[SATransaction] = None) -> Optional[TDomain]:
        obj_model = self.to_model(obj)
        query = (insert(self._model_class)
                 .values(obj_model.to_dict())
                 .on_conflict_do_nothing()
                 .returning(self._model_class))
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                created_model = result.first()
                await session.commit()
        else:
            result = await transaction.session.scalars(query)
            created_model = result.first()
        if created_model:
            return self.to_domain(created_model)

    async def create_all(self,
                         objs: List[TDomain],
                         transaction: Optional[SATransaction] = None) -> List[TDomain]:
        if not objs:
            return []
        obj_models = (self.to_model(obj) for obj in objs)
        query = (insert(self._model_class)
                 .values([obj_model.to_dict() for obj_model in obj_models])
                 .on_conflict_do_nothing()
                 .returning(self._model_class))
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                created_models = result.all()
                await session.commit()
        else:
            result = await transaction.session.scalars(query)
            created_models = result.all()
        return [self.to_domain(created_model) for created_model in created_models]

    async def update(self,
                     obj_pk: TPK,
                     fields: UpdateFields,
                     transaction: Optional[SATransaction] = None) -> Optional[TDomain]:
        model_pk = self.pk_to_model_pk(obj_pk)
        query = update(self._model_class).filter_by(**model_pk).values(fields.to_dict()).returning(self._model_class)
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                updated_model = result.first()
                await session.commit()
        else:
            result = await transaction.session.scalars(query)
            updated_model = result.first()
        if updated_model:
            return self.to_domain(updated_model)

    async def update_all(self,
                         fields_by_obj_pk: Dict[TPK, UpdateFields],
                         transaction: Optional[SATransaction] = None) -> None:
        if not fields_by_obj_pk:
            return
        query = update(self._model_class)
        query_payload = [
            dict(**self.pk_to_model_pk(obj_pk), **update_fields.to_dict())
            for obj_pk, update_fields in fields_by_obj_pk.items()
        ]
        if not transaction:
            async with self._database.session as session:
                await session.execute(query, query_payload)
                await session.commit()
        else:
            await transaction.session.execute(query, query_payload)

    async def get(self,
                  obj_pk: TPK,
                  transaction: Optional[SATransaction] = None) -> Optional[TDomain]:
        model_pk = self.pk_to_model_pk(obj_pk)
        query = select(self._model_class).filter_by(**model_pk)
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                model = result.first()
        else:
            result = await transaction.session.scalars(query)
            model = result.first()
        if model:
            return self.to_domain(model)

    async def get_all(self,
                      transaction: Optional[SATransaction] = None) -> List[TDomain]:
        query = select(self._model_class)
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                models = result.all()
        else:
            result = await transaction.session.scalars(query)
            models = result.all()
        return [self.to_domain(model) for model in models]

    async def paginated(self,
                        pagination_query: PaginationQuery,
                        transaction: Optional[SATransaction] = None) -> List[TDomain]:
        query = select(self._model_class)
        if pagination_query.filter_fields_dnf:
            sqlalchemy_dnf = filter_fields_dnf_as_sqlalchemy_dnf(pagination_query.filter_fields_dnf, self._model_class)
            query = query.where(sqlalchemy_dnf)
        if pagination_query.order_by:
            order_column = getattr(self._model_class, pagination_query.order_by)
            if pagination_query.asc_sort:
                query = query.order_by(asc(order_column))
            else:
                query = query.order_by(desc(order_column))
        if pagination_query.limit_per_page:
            query = query.limit(pagination_query.limit_per_page)
        if pagination_query.offset_page:
            query = query.offset(pagination_query.offset_page)
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                models = result.all()
        else:
            result = await transaction.session.scalars(query)
            models = result.all()
        return [self.to_domain(model) for model in models]

    async def filter(self,
                     filter_fields_dnf: FilterFieldsDNF,
                     transaction: Optional[SATransaction] = None) -> List[TDomain]:
        sqlalchemy_dnf = filter_fields_dnf_as_sqlalchemy_dnf(filter_fields_dnf, self._model_class)
        query = select(self._model_class).where(sqlalchemy_dnf)
        if not transaction:
            async with self._database.session as session:
                result = await session.scalars(query)
                models = result.all()
        else:
            result = await transaction.session.scalars(query)
            models = result.all()
        return [self.to_domain(model) for model in models]

    async def count_by_fields(self,
                              filter_fields_dnf: FilterFieldsDNF,
                              transaction: Optional[SATransaction] = None) -> int:
        sqlalchemy_dnf = filter_fields_dnf_as_sqlalchemy_dnf(filter_fields_dnf, self._model_class)
        query = select(func.count()).select_from(self._model_class).where(sqlalchemy_dnf)
        if not transaction:
            async with self._database.session as session:
                value = await session.scalar(query)
        else:
            value = await transaction.session.scalar(query)
        return value


def filter_fields_dnf_as_sqlalchemy_dnf(filter_fields_dnf: FilterFieldsDNF, model_class: Type[Base]) -> ColumnElement:
    conjunctions = [conjunct_as_sqlalchemy_conjunct(conjunct, model_class) for conjunct in
                    filter_fields_dnf.conjunctions]
    dnf = or_(*conjunctions)
    return dnf


def conjunct_as_sqlalchemy_conjunct(conjunct: FilterFieldsConjunct, model_class: Type[Base]) -> ColumnElement:
    literals = [filter_field_as_sqlalchemy_literal(filter_field, model_class) for filter_field in conjunct.group]
    conj = and_(*literals)
    return conj


def filter_field_as_sqlalchemy_literal(filter_field: FilterField, model_class: Type[Base]) -> ColumnElement:
    column: ColumnElement = getattr(model_class, filter_field.name)
    if filter_field.operation == ConditionOperation.EQ:
        return eq(column, filter_field.value)
    if filter_field.operation == ConditionOperation.GT:
        return gt(column, filter_field.value)
    if filter_field.operation == ConditionOperation.GTE:
        return ge(column, filter_field.value)
    if filter_field.operation == ConditionOperation.LT:
        return lt(column, filter_field.value)
    if filter_field.operation == ConditionOperation.LTE:
        return le(column, filter_field.value)
    if filter_field.operation == ConditionOperation.IS_NULL:
        return column.is_(None)
    if filter_field.operation == ConditionOperation.NOT_NULL:
        return column.is_not(None)
    if filter_field.operation == ConditionOperation.IN:
        return column.in_(filter_field.value)
    raise RuntimeError(f"Unknown operation type: {filter_field.operation}")

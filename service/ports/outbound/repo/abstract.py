from abc import ABC, abstractmethod
from typing import List, Dict, TypeVar, Generic, Optional, Set

from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, UpdateFields
from service.ports.outbound.repo.transaction import Transaction

TDomain = TypeVar('TDomain')  # Domain объект
TModel = TypeVar('TModel')  # ORM/Database модель
TPK = TypeVar('TPK')  # Тип первичного ключа


class Repo(ABC, Generic[TDomain, TModel, TPK]):

    @abstractmethod
    def to_model(self, obj: TDomain) -> TModel:
        """ Метод для конвертации объекта домена в модель хранения """
        pass

    @abstractmethod
    def to_domain(self, obj: TModel) -> TDomain:
        """ Метод для конвертации модели хранения в объект домена """
        pass

    @abstractmethod
    async def create(self,
                     obj: TDomain,
                     transaction: Optional[Transaction] = None) -> Optional[TDomain]:
        """ Создает объект. Если объект уже существует, то игнорирует его создание """
        pass

    @abstractmethod
    async def create_all(self,
                         objs: List[TDomain] | Set[TDomain],
                         transaction: Optional[Transaction] = None) -> List[TDomain]:
        """ Создает объекты коллекции. Если объект уже существует, то игнорирует его создание """
        pass

    @abstractmethod
    async def update(self,
                     obj_pk: TPK,
                     fields: UpdateFields,
                     transaction: Optional[Transaction] = None) -> Optional[TDomain]:
        """Обновляет атрибуты объекта по первичному ключу."""
        pass

    @abstractmethod
    async def update_all(self,
                         fields_by_obj_pk: Dict[TPK, UpdateFields],
                         transaction: Optional[Transaction] = None) -> None:
        """Пакетно обновляет атрибуты объектов по их первичным ключам."""
        pass

    @abstractmethod
    async def get(self,
                  obj_pk: TPK,
                  transaction: Optional[Transaction] = None) -> Optional[TDomain]:
        """ Возвращает объект по его первичному ключу из коллекции """
        pass

    @abstractmethod
    async def get_all(self,
                      transaction: Optional[Transaction] = None) -> List[TDomain]:
        """ Возвращает все объекты в коллекции """
        pass

    @abstractmethod
    async def paginated(self,
                        pagination_query: PaginationQuery,
                        transaction: Optional[Transaction] = None) -> List[TDomain]:
        """ Возвращает результат запроса к коллекции объектов пачками в соответствии с переданными
         настройками фильтрации данных. Если не указаны размеры пагинации, то отдается вся коллекция объектов,
         подходящая под условие, что равносильно filter. Если не указано и условие, то результат равносилен вызову
         метода get_all """
        pass

    @abstractmethod
    async def filter(self,
                     filter_fields_dnf: FilterFieldsDNF,
                     transaction: Optional[Transaction] = None) -> List[TDomain]:
        pass

    @abstractmethod
    async def count_by_fields(self,
                              filter_fields_dnf: FilterFieldsDNF,
                              transaction: Optional[Transaction] = None) -> int:
        pass

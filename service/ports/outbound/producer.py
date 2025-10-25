from abc import ABC, abstractmethod
from typing import Type, List, Dict

from pydantic import BaseModel

from service.ports.common.convert_utils import to_bytes


class DataProducerI(ABC):

    async def produce_list(self,
                           items: List[bytes | dict | str | Type[BaseModel]],
                           to: str,
                           headers: List[dict] = None,
                           item_params: Dict = None):
        for index, item in enumerate(items):
            await self.produce(item, to, headers=headers, item_params=item_params)

    @abstractmethod
    async def produce(self, item: bytes | dict | str | Type[BaseModel],
                      to: str, headers: dict = None, item_params: dict = None) -> bool:
        pass


class DirectDataProducer(DataProducerI):

    def __init__(self, to: str, producer: DataProducerI):
        self._to = to
        self._producer = producer

    async def produce(self, item: BaseModel, to: str = None, headers: dict = None, item_params: dict = None, ) -> bool:
        item_encoded = to_bytes(item)
        if not to:
            target_to = self._to
        else:
            target_to = to
        return await self._producer.produce(item_encoded, target_to, headers, item_params)

    async def produce_list(self,
                           items: List[bytes | dict | str | BaseModel],
                           to: str = None,
                           headers: List[dict] = None,
                           item_params: List[dict] = None) :
        for index, item in enumerate(items):
            await self.produce(item, to, headers=headers, item_params=item_params)

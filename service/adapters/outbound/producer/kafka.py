import asyncio
from typing import Type, List, Tuple, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import MessageSizeTooLargeError
from pydantic import BaseModel

from service.adapters.outbound.producer.settings import KafkaProducerConnectionSettings, KafkaProducerSettings
from service.ports.common.convert_utils import to_bytes
from service.ports.common.interfaces import Startable
from service.ports.common.logs import logger
from service.ports.outbound.producer import DataProducerI


def prepare_headers(headers: dict) -> Optional[List[Tuple[str, bytes]]]:
    if not headers:
        return
    prepared_headers = []
    for key, value in headers.items():
        pair = (key, to_bytes(value))
        prepared_headers.append(pair)
    return prepared_headers


ONE_MEGABYTE = 1_048_567


class AiokafkaProducerConnection(Startable):
    def __init__(self, bootstrap_servers: str, retry_timeout: float):
        self._bootstrap_servers = bootstrap_servers
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        self._retry_timeout = retry_timeout

    @classmethod
    def from_settings(cls, settings: KafkaProducerConnectionSettings):
        return cls(bootstrap_servers=settings.bootstrap_servers, retry_timeout=settings.retry_timeout, )

    @property
    def producer(self) -> AIOKafkaProducer:
        return self._producer

    async def start(self):
        try:
            await self._producer.start()
        except BaseException as e:
            logger.warning(
                f'Connection to Kafka {self._bootstrap_servers} failed: {e}; retry after {self._retry_timeout} s')
            await asyncio.sleep(self._retry_timeout)
            await self.start()
        else:
            logger.info(f'Connection to Kafka {self._bootstrap_servers} created')

    async def stop(self):
        await self._producer.stop()
        logger.info(f'Connection to Kafka {self._bootstrap_servers} closed')


class AiokafkaProducer(DataProducerI):

    def __init__(self, connection: AiokafkaProducerConnection,
                 topic: str,
                 max_retries: int,
                 retry_timeout: float,
                 message_max_size: Optional[int], ):
        self._connection = connection
        self._topic = topic
        self._max_retries = max_retries
        self._retry_timeout = retry_timeout
        self._message_max_size = message_max_size if message_max_size else ONE_MEGABYTE

    @classmethod
    def from_settings(cls, settings: KafkaProducerSettings, connection: AiokafkaProducerConnection):
        return cls(connection=connection,
                   topic=settings.topic,
                   max_retries=settings.max_retries,
                   retry_timeout=settings.retry_timeout,
                   message_max_size=settings.message_max_size, )

    async def produce(self,
                      item: bytes | dict | str | Type[BaseModel],
                      to: str,
                      headers: dict = None,
                      item_params: dict = None) -> bool:
        exception = None
        prepared_headers = prepare_headers(headers)
        prepared_item = to_bytes(item)
        topic = to if to else self._topic
        if len(prepared_item) > self._message_max_size:
            logger.error(f"message size large than {self._message_max_size} bytes; skip message produce")
            return False
        for retry in range(self._max_retries):
            try:
                await self._connection.producer.send(topic=topic, value=prepared_item, headers=prepared_headers)
            except BaseException as e:
                if isinstance(e, MessageSizeTooLargeError):
                    logger.error(f"failed to produce message: too large size for broker: {e}")
                    return False
                logger.warning(
                    f"[{retry:2}|{self._max_retries}] failed to produce message: {e}; retry after: {self._retry_timeout} s")
                await asyncio.sleep(self._retry_timeout)
            else:
                return True
        logger.error(f'failed to produce message after {self._max_retries} retries: {exception}')
        return False

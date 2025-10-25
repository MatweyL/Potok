import asyncio
from typing import Type, Optional, Dict

from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractConnection, AbstractExchange, AbstractChannel
from pydantic import BaseModel

from service.adapters.outbound.producer.annotations import ExchangeType
from service.adapters.outbound.producer.settings import RMQProducerConnectionSettings, RMQProducerSettings
from service.ports.common.convert_utils import to_bytes
from service.ports.common.interfaces import Startable
from service.ports.common.logs import logger
from service.ports.outbound.dto import RabbitMQURI
from service.ports.outbound.producer import DataProducerI


class AioPikaRMQProducerConnection(Startable):
    def __init__(self, uri: RabbitMQURI, retry_timeout: float):
        self._uri = uri
        self._retry_timeout = retry_timeout
        self._connection: AbstractConnection = None

    @classmethod
    def from_settings(cls, settings: RMQProducerConnectionSettings):
        return cls(uri=RabbitMQURI.from_str(settings.uri), retry_timeout=settings.retry_timeout)

    async def start(self):
        try:
            self._connection = await connect_robust(self._uri.as_str)
        except BaseException as e:
            logger.warning(
                f'Connection to RMQ {self._uri.safe_info} failed: {e}; retry after {self._retry_timeout} s')
            await asyncio.sleep(self._retry_timeout)
            await self.start()
        else:
            logger.info(f'Connection to RMQ {self._uri.safe_info} created')

    async def stop(self):
        if not self.connection.is_closed:
            await self.connection.close()
            logger.info(f'Connection to RMQ {self._uri.safe_info} closed')
        else:
            logger.info(f'Connection to RMQ {self._uri.safe_info} already closed')

    @property
    def uri(self) -> RabbitMQURI:
        return self._uri

    @property
    def connection(self) -> AbstractConnection:
        if not self._connection:
            raise Exception(f"Connection doesn't exists to {self._uri.safe_info}")
        return self._connection


class AioPikaRMQProducer(DataProducerI, Startable):

    def __init__(self,
                 connection: AioPikaRMQProducerConnection,
                 exchange_name: str,
                 exchange_type: ExchangeType,
                 exchange_params: Optional[Dict],
                 routing_key: str,
                 max_retries: int,
                 retry_timeout: float, ):
        self._connection = connection
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._exchange_params = exchange_params or {}
        self._routing_key = routing_key
        self._max_retries = max_retries
        self._retry_timeout = retry_timeout

        self._channel: AbstractChannel = None
        self._exchange: AbstractExchange = None

    @classmethod
    def from_settings(cls, settings: RMQProducerSettings, connection: AioPikaRMQProducerConnection):
        return cls(connection=connection,
                   exchange_name=settings.exchange_name,
                   exchange_type=settings.exchange_type,
                   exchange_params=settings.exchange_params,
                   routing_key=settings.routing_key,
                   max_retries=settings.max_retries,
                   retry_timeout=settings.retry_timeout)

    async def start(self):
        self._channel = await self._connection.connection.channel()
        if not self._exchange_name:
            self._exchange = self._channel.default_exchange
        else:
            self._exchange = await self._channel.declare_exchange(self._exchange_name,
                                                                  self._exchange_type,
                                                                  **self._exchange_params)
        logger.info(f'RabbitMQ producer configured for {self._connection.uri.safe_info};'
                    f' exchange: {self._exchange.name}; routing_key: {self._routing_key}')

    async def stop(self):
        await self._channel.close()
        logger.info(f'RabbitMQ producer {self._connection.uri.safe_info} stopped')

    async def produce(self, item: bytes | dict | str | Type[BaseModel], to: str, headers: dict = None,
                      item_params: dict = None) -> bool:
        prepared_body = to_bytes(item)
        message = Message(body=prepared_body, headers=headers)
        routing_key = to if to else self._routing_key
        exception = None
        for retry in range(self._max_retries):
            try:
                await self._exchange.publish(message, routing_key=routing_key)
            except BaseException as e:
                logger.warning(
                    f"[{retry:2}|{self._max_retries}] failed to produce message: {e}; retry after: {self._retry_timeout} s")
                exception = e
                await asyncio.sleep(self._retry_timeout)
            else:
                return True
        logger.error(f'failed to produce message after {self._max_retries} retries: {exception}')
        return False

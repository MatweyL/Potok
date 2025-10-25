import asyncio
from typing import Callable, Awaitable, Dict, Any

from aio_pika import connect_robust
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractIncomingMessage, AbstractQueue, AbstractExchange

from service.adapters.inbound.consumer.settings import RMQConsumerConnectionSettings, RMQConsumerSettings
from service.ports.common.input_converter import InputConverterI
from service.ports.common.interfaces import Startable
from service.ports.common.logs import logger
from service.ports.outbound.dto import RabbitMQURI


class AioPikaRMQConsumerConnection(Startable):
    def __init__(self, uri: RabbitMQURI, retry_timeout: float):
        self._uri = uri
        self._retry_timeout = retry_timeout
        self._connection: AbstractConnection = None

    @classmethod
    def from_settings(cls, settings: RMQConsumerConnectionSettings):
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


class AioPikaRMQConsumer(Startable):
    def __init__(self, connection: AioPikaRMQConsumerConnection, prefetch_count: int):
        self._connection = connection
        self._prefetch_count = prefetch_count

        self._exchanges: Dict[tuple[str, str], AbstractExchange] = {}
        self._queues: Dict[str, AbstractQueue] = {}
        self._consumer_tag_by_queue_name: Dict[str, str] = {}
        self._channel: AbstractChannel = None

    @classmethod
    def from_settings(cls, settings: RMQConsumerSettings, connection: AioPikaRMQConsumerConnection):
        return cls(connection=connection, prefetch_count=settings.prefetch_count)

    async def start(self):
        self._channel = await self._connection.connection.channel()

        await self._channel.set_qos(prefetch_count=self._prefetch_count)
        logger.info(f'RabbitMQ consumer configured for {self._connection.uri.safe_info};'
                    f' prefetch count: {self._prefetch_count}')

    async def stop(self):
        await self._channel.close()
        logger.info(f'RabbitMQ consumer {self._connection.uri.safe_info} stopped')

    def _processing_callback_bridge(self, processing_callback: Callable[[dict], Awaitable],
                                    message_converter: InputConverterI):

        async def inner(message: AbstractIncomingMessage):
            async with message.process():
                message_str = message.body.decode('utf-8')
                message_converted = message_converter.convert(message_str)
            logger.debug(f'{self} got message')
            try:
                await processing_callback(message_converted)
            except BaseException as e:
                logger.exception(e)
                logger.error(f'{self} failed to process message: {e}')
            else:
                logger.debug(f'{self} processed message')

        return inner

    def is_queue_consumed(self, queue_name) -> bool:
        return queue_name in self._queues

    async def consume_queue(self,
                            queue_name: str,
                            processing_callback: Callable[[Any], Awaitable],
                            message_converter: InputConverterI, **queue_kwargs):
        queue = await self._channel.declare_queue(queue_name, **queue_kwargs)
        consumer_tag = await queue.consume(self._processing_callback_bridge(processing_callback, message_converter))
        self._queues[queue_name] = queue
        self._consumer_tag_by_queue_name[queue_name] = consumer_tag

    async def stop_consume_queue(self, queue_name: str, ):
        queue = self._queues[queue_name]
        consumer_tag = self._consumer_tag_by_queue_name[queue_name]
        await queue.cancel(consumer_tag)
        self._queues.pop(queue_name)
        self._consumer_tag_by_queue_name.pop(queue_name)

    async def bind_to_exchange(self, exchange_name: str, exchange_type: str, queue_name: str, routing_key: str,
                               headers: dict):
        try:
            exchange = self._exchanges[(exchange_name, exchange_type)]
        except KeyError:
            if exchange_name:
                exchange = await self._channel.declare_exchange(exchange_name, exchange_type, passive=True)
            else:
                exchange = self._channel.default_exchange
            self._exchanges[(exchange_name, exchange_type)] = exchange
        queue = self._queues[queue_name]
        await queue.bind(exchange, routing_key=routing_key, arguments=headers)

    async def unbind_from_exchange(self, exchange_name: str, exchange_type: str, queue_name: str, routing_key: str,
                                   headers: dict):
        try:
            exchange = self._exchanges[(exchange_name, exchange_type)]
        except KeyError:
            exchange = await self._channel.declare_exchange(exchange_name, exchange_type, passive=True)
            self._exchanges[(exchange_name, exchange_type)] = exchange
        queue = self._queues[queue_name]
        await queue.unbind(exchange, routing_key=routing_key, arguments=headers)


class RMQQueueConsumer(Startable):

    def __init__(self,
                 consumer: AioPikaRMQConsumer,
                 queue_name: str,
                 processing_callback: Callable[[Any], Awaitable],
                 message_converter: InputConverterI,
                 **queue_kwargs):
        self.consumer = consumer
        self.queue_name = queue_name
        self.processing_callback = processing_callback
        self.message_converter = message_converter
        self.queue_kwargs = queue_kwargs

    async def start(self):
        await self.consumer.consume_queue(self.queue_name,
                                          self.processing_callback,
                                          self.message_converter,
                                          **self.queue_kwargs)

    async def stop(self):
        await self.consumer.stop_consume_queue(self.queue_name)

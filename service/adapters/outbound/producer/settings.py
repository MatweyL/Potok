from typing import Optional, Dict

from pydantic_settings import BaseSettings

from service.adapters.outbound.producer.annotations import ExchangeType


class KafkaProducerConnectionSettings(BaseSettings):
    bootstrap_servers: str
    retry_timeout: float = 5


class KafkaProducerSettings(BaseSettings):
    topic: str
    max_retries: int = 3
    retry_timeout: float = 2
    message_max_size: Optional[int] = None


class RMQProducerConnectionSettings(BaseSettings):
    uri: str
    retry_timeout: float = 5


class RMQProducerSettings(BaseSettings):
    exchange_name: str = ''
    exchange_type: ExchangeType = 'direct'
    exchange_params: Optional[Dict] = None
    routing_key: Optional[str] = None
    max_retries: int = 3
    retry_timeout: float = 2

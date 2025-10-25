from typing import Dict, Optional

from pydantic_settings import BaseSettings


class RMQConsumerConnectionSettings(BaseSettings):
    uri: str
    retry_timeout: float = 5


class RMQConsumerSettings(BaseSettings):
    prefetch_count: int


class RMQQueueConsumerSettings(BaseSettings):
    name: str
    params: Optional[Dict] = None

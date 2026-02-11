from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from service.adapters.inbound.consumer.settings import RMQConsumerSettings, RMQConsumerConnectionSettings
from service.adapters.inbound.rest_api.settings import FastAPIServerSettings
from service.adapters.outbound.producer.settings import RMQProducerSettings, RMQProducerConnectionSettings


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore",
                                      env_nested_delimiter="__",
                                      env_file_encoding='utf-8',
                                      env_file=Path(__file__).parent.parent.joinpath('.env'), )
    fastapi_server: FastAPIServerSettings = Field(default_factory=FastAPIServerSettings)

    rmq_consumer_connection: RMQConsumerConnectionSettings
    rmq_consumer: RMQConsumerSettings
    rmq_task_run_execution_status_queue: str

    rmq_producer_connection: RMQProducerConnectionSettings
    rmq_producer_task_run: RMQProducerSettings

    database_uri: str

import enum
from pathlib import Path
from typing import Dict, Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from service.adapters.inbound.consumer.settings import RMQConsumerSettings, RMQConsumerConnectionSettings
from service.adapters.inbound.rest_api.settings import FastAPIServerSettings
from service.adapters.outbound.producer.settings import RMQProducerSettings, RMQProducerConnectionSettings
from service.ports.outbound.dto import URI


class ServiceType(str, enum.Enum):
    MONOLITH = "MONOLITH"
    API = "API"
    WORKER = "WORKER"


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
    ch_uri: str
    ch_secure: bool = False
    admin_username: str
    admin_password: str
    jwt_secret_key: str
    use_https: bool = False
    service_type: ServiceType = ServiceType.MONOLITH

    use_ch_task_run_status_log_repo: bool = False
    use_ch_task_run_time_interval_progress_repo: bool = False
    use_ch_task_run_time_interval_execution_bounds_repo: bool = False
    use_ch_time_interval_task_progress_repo: bool = False

    def ch_uri_as_params(self) -> Dict[str, Any]:
        uri = URI.from_str(self.ch_uri)
        return {
            "host": uri.host,
            "port": uri.port,
            "username": uri.user,
            "password": uri.password,
            "database": uri.resource,
            "secure": self.ch_secure,
            "verify": True,
        }

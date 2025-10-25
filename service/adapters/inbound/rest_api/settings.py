from typing import Optional, List

from pydantic_settings import BaseSettings

from service.adapters.inbound.rest_api.annotations import HTTPMethod


class FastAPIServerSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    title: Optional[str] = None
    description: Optional[str] = None
    origins: Optional[List[str]] = None
    allow_methods: Optional[List[HTTPMethod]] = None
    allow_headers: Optional[List[str]] = None
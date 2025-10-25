from typing import Dict, List, Optional

from pydantic import BaseModel

from service.ports.outbound.annotations import ReducibleToInt, ReducibleToLoweredStr


class URI(BaseModel):
    protocol: ReducibleToLoweredStr
    user: str
    password: str
    host: str
    port: Optional[ReducibleToInt]
    resource: Optional[str] = None

    @classmethod
    def from_str(cls, str_uri: str):
        protocol_end = str_uri.find(':')
        protocol = str_uri[:protocol_end]

        server_creds_delimiter = str_uri.find('@')

        server = str_uri[server_creds_delimiter + 1:]
        server_delimiter = server.find(':')
        server_end = server.find('/')
        if server_end == -1:
            server_end = len(server)
            resource = None
        else:
            resource = server[server_end + 1:]
        host = server[:server_delimiter]
        port = server[server_delimiter + 1:server_end]

        creds = str_uri[protocol_end + 3:server_creds_delimiter]
        creds_delimiter = creds.find(':')
        username = creds[:creds_delimiter]
        password = creds[creds_delimiter + 1:]
        return cls(protocol=protocol, user=username, password=password, host=host, port=port, resource=resource)


class RabbitMQURI(URI):

    @property
    def safe_info(self) -> str:
        return f"{self.host}:{self.port}/{self.resource}"

    @property
    def as_str(self) -> str:
        return f"{self.protocol}://{self.user}:{self.password}@{self.host}:{self.port}/{self.resource}"

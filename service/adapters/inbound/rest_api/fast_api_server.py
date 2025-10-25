import asyncio
from asyncio import Task
from typing import List, Union

import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from service.ports.common.interfaces import Startable
from service.ports.common.logs import logger
from .annotations import HTTPMethod, EveryType
from .router import router as api_router
from .settings import FastAPIServerSettings

DEFAULT_ORIGINS = ["*"]
DEFAULT_ALLOW_METHODS = ["*"]
DEFAULT_ALLOW_HEADERS = ["*"]

DEFAULT_TITLE = "Rest API Template"
DEFAULT_DESCRIPTION = "Template for creation Rest API"


class FastAPIServer(Startable):

    def __init__(self,
                 host: str,
                 port: int,
                 title: str,
                 description: str,
                 origins: List[str],
                 allow_methods: List[Union[HTTPMethod, EveryType]],
                 allow_headers: List[str], ):
        self._host = host
        self._port = port
        self._title = title
        self._description = description
        self._origins = origins
        self._allow_methods = allow_methods
        self._allow_headers = allow_headers

        self._app = FastAPI(title=title, description=description)
        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=self._allow_methods,
            allow_headers=self._allow_headers,
        )
        self._app.include_router(api_router)
        handle_422_exceptions(self._app)

        self._server_task: Task = None

    @property
    def app(self) -> FastAPI:
        return self._app

    @classmethod
    def from_settings(cls, settings: FastAPIServerSettings):
        return cls(host=settings.host,
                   port=settings.port,
                   title=settings.title if settings.title else DEFAULT_TITLE,
                   description=settings.description if settings.description else DEFAULT_DESCRIPTION,
                   origins=settings.origins if settings.origins else DEFAULT_ORIGINS,
                   allow_methods=settings.allow_methods if settings.allow_methods else DEFAULT_ALLOW_METHODS,
                   allow_headers=settings.allow_headers if settings.allow_headers else DEFAULT_ALLOW_HEADERS,)

    async def start(self):
        self._server_task = asyncio.create_task(self._run())
        logger.info(f'FastAPI started; local Swagger: http://localhost:{self._port}/docs')

    async def _run(self):
        config = uvicorn.Config(app=self._app, host=self._host, port=self._port)
        server = uvicorn.Server(config=config)
        await server.serve()


    async def stop(self):
        logger.warning("unable to stop Rest API server")


def handle_422_exceptions(app: FastAPI):
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
        logger.error(exc_str)
        content = {'status_code': 10422, 'message': exc_str, 'data': None}
        return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

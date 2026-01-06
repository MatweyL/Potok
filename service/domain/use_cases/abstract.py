from abc import ABC, abstractmethod
from typing import Optional, Annotated, Union

from pydantic import BaseModel, BeforeValidator


class UCRequest(BaseModel):
    pass


def exception_as_text(e: BaseException) -> Optional[str]:
    if not e:
        return
    return f"{e.__class__.__name__}: {e}"


class UCResponse(BaseModel):
    request: UCRequest
    success: bool
    error: Annotated[Optional[Union[str]], BeforeValidator(exception_as_text)] = None


class UseCase(ABC):

    @abstractmethod
    async def apply(self, request: UCRequest) -> UCResponse:
        pass

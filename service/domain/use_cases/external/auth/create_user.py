from datetime import datetime
from typing import Optional, List

from service.domain.schemas.app_user import AppUser, AppUserPK, AppUserDTO
from service.domain.schemas.enums import AppUserRole
from service.domain.services.hasher import Hasher
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class CreateUserUCRq(UCRequest):
    username: str
    password: str
    roles: Optional[List[AppUserRole]] = None


class CreateUserUCRs(UCResponse):
    request: CreateUserUCRq
    app_user_dto: Optional[AppUserDTO] = None


class CreateUserUC(UseCase):
    """
    Юз-кейс создания пользователя, вызывается администратором системы.
    Изначально в системе всегда существует один администратор
    """
    def __init__(self, app_user_repo: Repo[AppUser, AppUser, AppUserPK],
                 hasher: Hasher):
        self._app_user_repo = app_user_repo
        self._hasher = hasher

    async def apply(self, request: CreateUserUCRq) -> CreateUserUCRs:
        users = await self._app_user_repo.filter(FilterFieldsDNF.single('username', request.username))
        if users:
            return CreateUserUCRs(success=False, error="Username unavailable", request=request,)
        roles = request.roles if request.roles else [AppUserRole.OPERATOR]
        password_hash = self._hasher.hash(request.password)
        app_user = AppUser(roles=roles, username=request.username, password_hash=password_hash,
                           created_at=datetime.now())
        app_user = await self._app_user_repo.create(app_user)
        return CreateUserUCRs(success=True,
                              request=request,
                              app_user_dto=AppUserDTO.from_app_user(app_user))

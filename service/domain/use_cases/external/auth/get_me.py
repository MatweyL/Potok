# service/domain/use_cases/get_me_uc.py

from typing import Optional

from service.domain.schemas.app_user import AppUser, AppUserPK, AppUserDTO
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class GetMeUCRq(UCRequest):
    user_id: int  # из декодированного токена, на уровне хэндлера


class GetMeUCRs(UCResponse):
    request: GetMeUCRq
    app_user_dto: Optional[AppUser] = None


class GetMeUC(UseCase):
    # TODO: Добавить TTL кеш. Юзеры будут меняться скорее редко, чем часто
    #  Если актуальность данных будет 30-60 сек, это кратно сократит количество запросов к хранилищу
    def __init__(self, app_user_repo: Repo[AppUser, AppUser, AppUserPK]):
        self._app_user_repo = app_user_repo

    async def apply(self, request: GetMeUCRq) -> GetMeUCRs:
        users = await self._app_user_repo.filter(
            FilterFieldsDNF.single("id", request.user_id)
        )
        if not users:
            return GetMeUCRs(success=False, error="User not found", request=request)

        user = users[0]

        if not user.is_active:
            return GetMeUCRs(success=False, error="Account is deactivated", request=request)

        return GetMeUCRs(
            success=True,
            app_user_dto=user,
            request=request,
        )
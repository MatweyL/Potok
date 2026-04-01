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
    app_user_dto: Optional[AppUserDTO] = None


class GetMeUC(UseCase):
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
            app_user_dto=AppUserDTO.from_app_user(user),
            request=request,
        )
# service/domain/use_cases/reset_password_uc.py

from typing import Optional

from service.domain.schemas.app_user import AppUser, AppUserPK, AppUserDTO
from service.domain.schemas.enums import AppUserRole
from service.domain.services.hasher import Hasher
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, UpdateFields


class ResetPasswordUCRq(UCRequest):
    requestor_roles: list[AppUserRole]  # из декодированного токена, на уровне хэндлера
    target_user_id: int
    new_password: str


class ResetPasswordUCRs(UCResponse):
    request: ResetPasswordUCRq
    app_user_dto: Optional[AppUserDTO] = None


class ResetPasswordUC(UseCase):
    def __init__(
            self,
            app_user_repo: Repo[AppUser, AppUser, AppUserPK],
            hasher: Hasher,
    ):
        self._app_user_repo = app_user_repo
        self._hasher = hasher

    async def apply(self, request: ResetPasswordUCRq) -> ResetPasswordUCRs:
        if AppUserRole.ADMIN not in request.requestor_roles:
            return ResetPasswordUCRs(
                success=False, error="Permission denied: ADMIN role required", request=request
            )

        users = await self._app_user_repo.filter(
            FilterFieldsDNF.single("id", request.target_user_id)
        )
        if not users:
            return ResetPasswordUCRs(
                success=False, error="User not found", request=request
            )

        target = users[0]
        target.password_hash = self._hasher.hash(request.new_password)
        updated = await self._app_user_repo.update(target, UpdateFields.single('password_hash', target.password_hash))

        return ResetPasswordUCRs(
            success=True,
            app_user_dto=AppUserDTO.from_app_user(updated),
            request=request,
        )

from service.domain.schemas.app_user import AppUser, AppUserPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields


class ActivateUserUCRq(UCRequest):
    target_user_id: int
    current_user_id: int


class ActivateUserUCRs(UCResponse):
    request: ActivateUserUCRq


class ActivateUserUC(UseCase):

    def __init__(self, app_user_repo: Repo[AppUser, AppUser, AppUserPK], ):
        self._app_user_repo = app_user_repo

    async def apply(self, request: ActivateUserUCRq) -> ActivateUserUCRs:
        if request.target_user_id == request.current_user_id:
            return ActivateUserUCRs(success=False, error="Self-activation is forbidden", request=request)
        app_user = await self._app_user_repo.get(AppUserPK(id=request.target_user_id))
        if not app_user:
            return ActivateUserUCRs(success=False, error="User not found", request=request)
        app_user = await self._app_user_repo.update(app_user, UpdateFields.single('is_active', True))
        success = app_user.is_active == True
        return ActivateUserUCRs(success=success, request=request)

from service.domain.schemas.app_user import AppUser, AppUserPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields


class DeactivateUserUCRq(UCRequest):
    target_user_id: int
    current_user_id: int


class DeactivateUserUCRs(UCResponse):
    request: DeactivateUserUCRq


class DeactivateUserUC(UseCase):

    def __init__(self, app_user_repo: Repo[AppUser, AppUser, AppUserPK], ):
        self._app_user_repo = app_user_repo

    async def apply(self, request: DeactivateUserUCRq) -> DeactivateUserUCRs:
        if request.target_user_id == request.current_user_id:
            return DeactivateUserUCRs(success=False, error="Self-deactivation is forbidden", request=request)
        app_user = await self._app_user_repo.get(AppUserPK(id=request.target_user_id))
        if not app_user:
            return DeactivateUserUCRs(success=False, error="User not found", request=request)
        app_user = await self._app_user_repo.update(app_user, UpdateFields.single('is_active', False))
        success = app_user.is_active == False
        return DeactivateUserUCRs(success=success, request=request)

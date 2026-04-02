from typing import List, Optional

from service.domain.schemas.app_user import AppUser, AppUserPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo


class GetAllUsersUCRq(UCRequest):
    pass


class GetAllUsersUCRs(UCResponse):
    request: GetAllUsersUCRq
    users: Optional[List[AppUser]] = None


class GetAllUsersUC(UseCase):
    def __init__(self, app_user_repo: Repo[AppUser, AppUser, AppUserPK], ):
        self._app_user_repo = app_user_repo

    async def apply(self, request: GetAllUsersUCRq) -> GetAllUsersUCRs:
        users = await self._app_user_repo.get_all()
        return GetAllUsersUCRs(success=True, request=request, users=users)

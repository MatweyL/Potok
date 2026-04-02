from service.domain.use_cases.external.admin.activate_user import ActivateUserUC, ActivateUserUCRq, ActivateUserUCRs
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUC, DeactivateUserUCRq, \
    DeactivateUserUCRs
from service.domain.use_cases.external.admin.get_all_users import GetAllUsersUC, GetAllUsersUCRs, GetAllUsersUCRq


class AdminUseCaseFacade:

    def __init__(self,
                 deactivate_user_uc: DeactivateUserUC,
                 get_all_users_uc: GetAllUsersUC,
                 activate_user_uc: ActivateUserUC,):
        self._deactivate_user_uc = deactivate_user_uc
        self._get_all_users_uc = get_all_users_uc
        self._activate_user_uc = activate_user_uc

    async def get_all_users(self) -> GetAllUsersUCRs:
        return await self._get_all_users_uc.apply(GetAllUsersUCRq())

    async def deactivate_user(self, request: DeactivateUserUCRq) -> DeactivateUserUCRs:
        return await self._deactivate_user_uc.apply(request)

    async def activate_user(self, request: ActivateUserUCRq) -> ActivateUserUCRs:
        return await self._activate_user_uc.apply(request)

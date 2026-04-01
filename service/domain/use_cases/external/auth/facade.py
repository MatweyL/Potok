from service.domain.use_cases.external.auth.login import LoginUC, LoginUCRq, LoginUCRs
from service.domain.use_cases.external.auth.logout import LogoutUCRq, LogoutUCRs, LogoutUC
from service.domain.use_cases.external.auth.refresh_token import RefreshTokenUC, RefreshTokenUCRq, RefreshTokenUCRs


class AuthUseCaseFacade:

    def __init__(self, login: LoginUC, logout: LogoutUC, refresh_token: RefreshTokenUC,):
        self._login = login
        self._logout = logout
        self._refresh_token = refresh_token

    async def login(self, request: LoginUCRq) -> LoginUCRs:
        return await self._login.apply(request)

    async def logout(self, request: LogoutUCRq) -> LogoutUCRs:
        return await self._logout.apply(request)

    async def refresh_token(self, request: RefreshTokenUCRq) -> RefreshTokenUCRs:
        return await self._refresh_token.apply(request)

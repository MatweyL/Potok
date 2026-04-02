from service.domain.use_cases.external.auth.create_user import CreateUserUC, CreateUserUCRq, CreateUserUCRs
from service.domain.use_cases.external.auth.get_me import GetMeUC, GetMeUCRq, GetMeUCRs
from service.domain.use_cases.external.auth.login import LoginUC, LoginUCRq, LoginUCRs
from service.domain.use_cases.external.auth.logout import LogoutUCRq, LogoutUCRs, LogoutUC
from service.domain.use_cases.external.auth.refresh_token import RefreshTokenUC, RefreshTokenUCRq, RefreshTokenUCRs
from service.domain.use_cases.external.auth.reset_password import ResetPasswordUC, ResetPasswordUCRq, ResetPasswordUCRs


class AuthUseCaseFacade:

    def __init__(self, login: LoginUC, logout: LogoutUC, refresh_token: RefreshTokenUC,
                 get_me_uc: GetMeUC,
                 create_user_uc: CreateUserUC,
                 reset_password_uc: ResetPasswordUC,
                 ):
        self._login = login
        self._logout = logout
        self._refresh_token = refresh_token
        self._get_me_uc = get_me_uc
        self._create_user_uc = create_user_uc
        self._reset_password_uc = reset_password_uc

    async def login(self, request: LoginUCRq) -> LoginUCRs:
        return await self._login.apply(request)

    async def logout(self, request: LogoutUCRq) -> LogoutUCRs:
        return await self._logout.apply(request)

    async def refresh_token(self, request: RefreshTokenUCRq) -> RefreshTokenUCRs:
        return await self._refresh_token.apply(request)

    async def get_me(self, request: GetMeUCRq) -> GetMeUCRs:
        return await self._get_me_uc.apply(request)

    async def reset_password(self, request: ResetPasswordUCRq) -> ResetPasswordUCRs:
        return await self._reset_password_uc.apply(request)

    async def create_user(self, request: CreateUserUCRq) -> CreateUserUCRs:
        return await self._create_user_uc.apply(request)
# service/domain/use_cases/login_uc.py

from service.domain.schemas.app_user import AppUserDTO, AppUser, AppUserPK
from service.domain.schemas.refresh_token import RefreshToken
from service.domain.services.hasher import Hasher
from service.domain.services.token_service import TokenService
from service.domain.use_cases.abstract import UCRequest, UCResponse, UseCase
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class LoginUCRq(UCRequest):
    username: str
    password: str


class LoginUCRs(UCResponse):
    request: LoginUCRq
    access_token: str | None = None
    refresh_token: str | None = None  # хэндлер положит это в httpOnly cookie
    user: AppUserDTO | None = None


class LoginUC(UseCase):
    def __init__(
        self,
        app_user_repo: Repo[AppUser, AppUser, AppUserPK],
        refresh_token_repo: Repo[RefreshToken, RefreshToken, ...],
        hasher: Hasher,
        token_service: TokenService,
    ):
        self._app_user_repo = app_user_repo
        self._refresh_token_repo = refresh_token_repo
        self._hasher = hasher
        self._token_service = token_service

    async def apply(self, request: LoginUCRq) -> LoginUCRs:
        users = await self._app_user_repo.filter(
            FilterFieldsDNF.single("username", request.username)
        )
        if not users:
            return LoginUCRs(success=False, error="Invalid credentials", request=request)

        user = users[0]

        if not self._hasher.verify(request.password, user.password_hash):
            return LoginUCRs(success=False, error="Invalid credentials", request=request)

        if not user.is_active:
            return LoginUCRs(success=False, error="Account is deactivated", request=request)

        access_token, refresh_token = self._token_service.issue_token_pair(user)

        # Сохраняем refresh в БД
        await self._refresh_token_repo.create(
            self._token_service.build_refresh_token_entity(user, refresh_token)
        )

        return LoginUCRs(
            success=True,
            access_token=access_token,
            refresh_token=refresh_token,
            user=AppUserDTO.from_app_user(user),
            request=request,
        )

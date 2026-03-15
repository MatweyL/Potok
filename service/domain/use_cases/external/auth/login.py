from service.domain.schemas.app_user import AppUserDTO, AppUser, AppUserPK
from service.domain.services.hasher import Hasher
from service.domain.services.token_service import TokenService
from service.domain.use_cases.abstract import UCRequest, UCResponse, UseCase
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class LoginUCRq(UCRequest):
    username: str
    password: str


class LoginUCRs(UCResponse):
    access_token: str | None = None
    refresh_token: str | None = None
    user: AppUserDTO | None = None


class LoginUC(UseCase):
    def __init__(
        self,
        app_user_repo: Repo[AppUser, AppUser, AppUserPK],
        hasher: Hasher,
        token_service: TokenService,          # ваш сервис по работе с JWT
    ):
        self._app_user_repo = app_user_repo
        self._hasher = hasher
        self._token_service = token_service

    async def apply(self, request: LoginUCRq) -> LoginUCRs:
        users = await self._app_user_repo.filter(
            FilterFieldsDNF.single("username", request.username)
        )
        if not users:
            return LoginUCRs(error="Invalid credentials", request=request, success=False)

        user = users[0]
        if not self._hasher.verify(request.password, user.password_hash):
            return LoginUCRs(error="Invalid credentials",  request=request, success=False)

        if not user.is_active:  # если добавите такое поле
            return LoginUCRs(error="Account is deactivated",  request=request, success=False)

        access_token = self._token_service.create_access_token(user)
        refresh_token = self._token_service.create_refresh_token(user)

        # можно сохранить refresh в БД / Redis, если используете blacklist
        # await self._token_service.store_refresh_token(user.id, refresh_token)

        return LoginUCRs(
            success=True,
            access_token=access_token,
            refresh_token=refresh_token,
            user=AppUserDTO.from_app_user(user),
            request=request,
        )

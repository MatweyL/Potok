# service/domain/use_cases/refresh_token_uc.py

from datetime import datetime, timezone

from service.domain.schemas.app_user import AppUser, AppUserPK, AppUserDTO
from service.domain.schemas.refresh_token import RefreshToken
from service.domain.services.token_service import TokenService
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class RefreshTokenUCRq(UCRequest):
    refresh_token: str  # хэндлер достаёт из httpOnly cookie и передаёт сюда


class RefreshTokenUCRs(UCResponse):
    request: RefreshTokenUCRq
    access_token: str | None = None
    refresh_token: str | None = None  # хэндлер обновит cookie
    user: AppUserDTO | None = None


class RefreshTokenUC(UseCase):
    def __init__(
        self,
        app_user_repo: Repo[AppUser, AppUser, AppUserPK],
        refresh_token_repo: Repo[RefreshToken, RefreshToken, ...],
        token_service: TokenService,
    ):
        self._app_user_repo = app_user_repo
        self._refresh_token_repo = refresh_token_repo
        self._token_service = token_service

    async def apply(self, request: RefreshTokenUCRq) -> RefreshTokenUCRs:
        # 1. Валидируем подпись и срок
        try:
            payload = self._token_service.decode_token(
                request.refresh_token, expected_type="refresh"
            )
        except ValueError as e:
            return RefreshTokenUCRs(success=False, error=str(e), request=request)

        # 2. Проверяем, что токен есть в БД (не был отозван через logout)
        stored = await self._refresh_token_repo.filter(
            FilterFieldsDNF.single("token", request.refresh_token)
        )
        if not stored:
            # Токен валиден криптографически, но отозван — возможная кража
            return RefreshTokenUCRs(
                success=False, error="Refresh token revoked", request=request
            )

        stored_token = stored[0]

        # 3. Проверяем срок в БД (страховка)
        if stored_token.expires_at < datetime.now():
            await self._refresh_token_repo.delete(stored_token)
            return RefreshTokenUCRs(
                success=False, error="Refresh token expired", request=request
            )

        # 4. Достаём пользователя и проверяем is_active
        users = await self._app_user_repo.filter(
            FilterFieldsDNF.single("id", int(payload["sub"]))
        )
        if not users:
            return RefreshTokenUCRs(success=False, error="User not found", request=request)

        user = users[0]

        if not user.is_active:
            await self._refresh_token_repo.delete(stored_token)
            return RefreshTokenUCRs(
                success=False, error="Account is deactivated", request=request
            )

        # 5. Ротация: старый токен удаляем, выдаём новую пару
        await self._refresh_token_repo.delete(stored_token)
        new_access, new_refresh = self._token_service.issue_token_pair(user)
        await self._refresh_token_repo.create(
            self._token_service.build_refresh_token_entity(user, new_refresh)
        )

        return RefreshTokenUCRs(
            success=True,
            access_token=new_access,
            refresh_token=new_refresh,
            user=AppUserDTO.from_app_user(user),
            request=request,
        )
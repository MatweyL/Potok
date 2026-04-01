# service/domain/use_cases/logout_uc.py

from service.domain.schemas.refresh_token import RefreshToken
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class LogoutUCRq(UCRequest):
    refresh_token: str  # хэндлер достаёт из httpOnly cookie


class LogoutUCRs(UCResponse):
    request: LogoutUCRq


class LogoutUC(UseCase):
    def __init__(self, refresh_token_repo: Repo[RefreshToken, RefreshToken, ...]):
        self._refresh_token_repo = refresh_token_repo

    async def apply(self, request: LogoutUCRq) -> LogoutUCRs:
        stored = await self._refresh_token_repo.filter(
            FilterFieldsDNF.single("token", request.refresh_token)
        )
        if stored:
            await self._refresh_token_repo.delete(stored[0])
        # Если токена нет — не ошибка, пользователь уже разлогинен

        return LogoutUCRs(success=True, request=request)

# service/domain/use_cases/external/api_token.py

import hashlib
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.util import await_only

from service.domain.schemas.api_token import ApiToken, ApiTokenCreate, ApiTokenPK, generate_api_key
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation, UpdateFields


# ─────────────────────────────────────────────────────────────────────────────
# Утилита хэширования
# ─────────────────────────────────────────────────────────────────────────────

def _hash_key(raw_key: str) -> str:
    """SHA-256 хэш ключа. Достаточно для API-ключей (не пароли)."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# CreateApiTokenUC
# ─────────────────────────────────────────────────────────────────────────────

class CreateApiTokenUCRq(UCRequest):
    name: str
    user_id: int
    expires_at: Optional[datetime] = None


class CreateApiTokenUCRs(UCResponse):
    request: CreateApiTokenUCRq
    api_token_create: Optional[ApiTokenCreate] = None


class CreateApiTokenUC(UseCase):
    """
    Создаёт новый API-ключ.
    Сырой ключ возвращается только один раз — потом восстановить невозможно.
    """

    def __init__(self, api_token_repo: Repo[ApiToken, ApiToken, ApiTokenPK]):
        self._repo = api_token_repo

    async def apply(self, request: CreateApiTokenUCRq) -> CreateApiTokenUCRs:
        raw_key = generate_api_key()

        token = await self._repo.create(ApiToken(
            name=request.name,
            key_prefix=raw_key[:12],   # "ptk_" + 8 символов
            key_hash=_hash_key(raw_key),
            user_id=request.user_id,
            is_active=True,
            created_at=datetime.now(timezone.utc),
            expires_at=request.expires_at,
        ))

        return CreateApiTokenUCRs(
            success=True,
            request=request,
            api_token_create=ApiTokenCreate(token=token, raw_key=raw_key),
        )


# ─────────────────────────────────────────────────────────────────────────────
# VerifyApiTokenUC  — вызывается из AuthMiddleware на каждый запрос
# ─────────────────────────────────────────────────────────────────────────────

class VerifyApiTokenUCRq(UCRequest):
    raw_key: str


class VerifyApiTokenUCRs(UCResponse):
    request: VerifyApiTokenUCRq
    is_valid: bool = False
    token: Optional[ApiToken] = None


class VerifyApiTokenUC(UseCase):
    """
    Проверяет API-ключ:
      1. Ищем по key_prefix (быстро, индекс)
      2. Сравниваем хэш
      3. Проверяем is_active и expires_at
      4. Обновляем last_used_at
    """

    def __init__(self, api_token_repo: Repo[ApiToken, ApiToken, ApiTokenPK]):
        self._repo = api_token_repo

    async def apply(self, request: VerifyApiTokenUCRq) -> VerifyApiTokenUCRs:
        raw_key = request.raw_key
        key_prefix = raw_key[:12]
        key_hash = _hash_key(raw_key)

        # Ищем по префиксу — быстрый запрос по индексированному полю
        candidates = await self._repo.filter(
            FilterFieldsDNF.single("key_prefix", key_prefix)
        )

        token = next(
            (t for t in candidates if t.key_hash == key_hash),
            None,
        )

        if token is None:
            return VerifyApiTokenUCRs(success=True, request=request, is_valid=False)

        # Проверяем активность и срок действия
        now = datetime.now(timezone.utc)
        if not token.is_active:
            return VerifyApiTokenUCRs(success=True, request=request, is_valid=False)
        if token.expires_at and token.expires_at < now:
            return VerifyApiTokenUCRs(success=True, request=request, is_valid=False)

        # Обновляем last_used_at асинхронно (не блокируем ответ)
        await self._repo.update(
            ApiTokenPK(id=token.id),
            UpdateFields.multiple({"last_used_at": now}),
        )

        return VerifyApiTokenUCRs(success=True, request=request, is_valid=True, token=token)


# ─────────────────────────────────────────────────────────────────────────────
# GetApiTokensUC  — список всех ключей для UI администратора
# ─────────────────────────────────────────────────────────────────────────────

class GetApiTokensUCRq(UCRequest):
    user_id: Optional[int] = None   # None = все ключи, int = ключи конкретного пользователя


class GetApiTokensUCRs(UCResponse):
    request: GetApiTokensUCRq
    tokens: List[ApiToken] = []


class GetApiTokensUC(UseCase):
    """Возвращает список API-ключей. Хэши и сырые ключи не возвращаются."""

    def __init__(self, api_token_repo: Repo[ApiToken, ApiToken, ApiTokenPK]):
        self._repo = api_token_repo

    async def apply(self, request: GetApiTokensUCRq) -> GetApiTokensUCRs:
        if request.user_id is not None:
            tokens = await self._repo.filter(
                FilterFieldsDNF.single("user_id", request.user_id)
            )
        else:
            tokens = await self._repo.get_all()

        # Скрываем хэш — он не нужен UI
        for t in tokens:
            t.key_hash = "***"

        return GetApiTokensUCRs(success=True, request=request, tokens=tokens)


# ─────────────────────────────────────────────────────────────────────────────
# RevokeApiTokenUC  — отзыв ключа администратором
# ─────────────────────────────────────────────────────────────────────────────

class RevokeApiTokenUCRq(UCRequest):
    token_id: int


class RevokeApiTokenUCRs(UCResponse):
    request: RevokeApiTokenUCRq
    token: Optional[ApiToken] = None


class RevokeApiTokenUC(UseCase):
    """Деактивирует ключ (is_active=False). Ключ остаётся в БД для аудита."""

    def __init__(self, api_token_repo: Repo[ApiToken, ApiToken, ApiTokenPK]):
        self._repo = api_token_repo

    async def apply(self, request: RevokeApiTokenUCRq) -> RevokeApiTokenUCRs:
        token = await self._repo.update(
            ApiTokenPK(id=request.token_id),
            UpdateFields.multiple({"is_active": False}),
        )
        if not token:
            return RevokeApiTokenUCRs(success=False, request=request,
                                      error="API token not found")
        return RevokeApiTokenUCRs(success=True, request=request, token=token)


# ─────────────────────────────────────────────────────────────────────────────
# DeleteApiTokenUC  — полное удаление ключа администратором
# ─────────────────────────────────────────────────────────────────────────────

class DeleteApiTokenUCRq(UCRequest):
    token_id: int


class DeleteApiTokenUCRs(UCResponse):
    request: DeleteApiTokenUCRq


class DeleteApiTokenUC(UseCase):
    """Полностью удаляет ключ из БД."""

    def __init__(self, api_token_repo: Repo[ApiToken, ApiToken, ApiTokenPK]):
        self._repo = api_token_repo

    async def apply(self, request: DeleteApiTokenUCRq) -> DeleteApiTokenUCRs:
        deleted = await self._repo.delete(ApiTokenPK(id=request.token_id))
        if not deleted:
            return DeleteApiTokenUCRs(success=False, request=request,
                                      error="API token not found")
        return DeleteApiTokenUCRs(success=True, request=request)


class ApiTokenFacade:
    def __init__(self,
                 delete_api_token_uc: DeleteApiTokenUC,
                 revoke_api_token_uc: RevokeApiTokenUC,
                 create_api_token_uc: CreateApiTokenUC,
                 verify_api_token_uc:VerifyApiTokenUC,
                 get_api_tokens_uc: GetApiTokensUC,):
        self._delete_api_token_uc = delete_api_token_uc
        self._revoke_api_token_uc=revoke_api_token_uc
        self._create_api_token_uc=create_api_token_uc
        self._verify_api_token_uc=verify_api_token_uc
        self._get_api_tokens_uc=get_api_tokens_uc

    async def delete_api_token(self, rq: DeleteApiTokenUCRq) -> DeleteApiTokenUCRs:
        return await self._delete_api_token_uc.apply(rq)

    async def revoke_api_token(self, rq: RevokeApiTokenUCRq) -> RevokeApiTokenUCRs:
        return await self._revoke_api_token_uc.apply(rq)

    async def create_api_token(self, rq: CreateApiTokenUCRq) -> CreateApiTokenUCRs:
        return await self._create_api_token_uc.apply(rq)

    async def verify_api_token(self, rq: VerifyApiTokenUCRq) -> VerifyApiTokenUCRs:
        return await self._verify_api_token_uc.apply(rq)

    async def get_api_tokens(self, rq: GetApiTokensUCRq) -> GetApiTokensUCRs:
        return await self._get_api_tokens_uc.apply(rq)

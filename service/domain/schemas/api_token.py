# service/domain/schemas/api_token.py

import secrets
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


def generate_api_key() -> str:
    """Генерирует безопасный случайный API-ключ формата 'ptk_<32 случайных байта>'."""
    return f"ptk_{secrets.token_hex(32)}"


class ApiTokenPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, ApiTokenPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ApiToken(ApiTokenPK):
    name: str                          # Описание: "Ключ сервиса мониторинга"
    key_prefix: str                    # Первые 8 символов для идентификации (ptk_XXXX)
    key_hash: str                      # bcrypt/sha256 хэш полного ключа
    user_id: int                       # От чьего имени действует ключ
    is_active: bool = True
    created_at: datetime
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None  # None = бессрочный


class ApiTokenCreate(BaseModel):
    """Возвращается только один раз при создании — содержит сырой ключ."""
    token: ApiToken
    raw_key: str                       # Показать пользователю один раз и забыть
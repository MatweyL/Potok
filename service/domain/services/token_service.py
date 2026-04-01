# src/auth/infrastructure/token_service.py
# или src/core/security/token_service.py — как вам удобнее

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError

from service.domain.schemas.app_user import AppUser, AppUserDTO
from service.domain.schemas.refresh_token import RefreshToken


class TokenService:
    """
    Сервис по работе с JWT-токенами (access + refresh с ротацией).

    Лучшие практики 2026:
    - Access token: короткий lifetime (15–60 мин)
    - Refresh token: длинный lifetime + ротация при каждом использовании
    - Алгоритм: HS256 (симметричный) — самый распространённый для монолита
    - Payload минимальный + sub + roles + exp + iat + (jti опционально)
    """

    def __init__(self, jwt_secret_key, jwt_algorithm: str = "HS256",
                 access_token_expire_minutes: int = 60,
                 refresh_token_expire_days: int = 30,
                 ):
        self.secret_key = jwt_secret_key
        self.algorithm = jwt_algorithm

        self.access_token_expire_minutes = access_token_expire_minutes
        self.refresh_token_expire_days = refresh_token_expire_days

    def create_access_token(self, user: AppUser | AppUserDTO) -> str:
        """
        Создаёт access-токен (короткоживущий).
        """
        to_encode: Dict[str, Any] = {
            "sub": str(user.id),
            "username": user.username,
            "roles": [role.value for role in user.roles],  # enum → строки
            "iat": datetime.now(),
        }

        expire = datetime.now() + timedelta(minutes=self.access_token_expire_minutes)
        to_encode.update({"exp": expire})

        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt

    def create_refresh_token(self, user: AppUser | AppUserDTO) -> str:
        """
        Создаёт refresh-токен (длинный lifetime).
        Можно добавить jti (уникальный id) для будущей revocation.
        """
        to_encode: Dict[str, Any] = {
            "sub": str(user.id),
            "type": "refresh",  # чтобы отличать от access
            "iat": datetime.now(),
        }

        expire = datetime.now() + timedelta(days=self.refresh_token_expire_days)
        to_encode.update({"exp": expire})

        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt

    def decode_token(self, token: str, expected_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Декодирует и валидирует токен.
        Возвращает payload или выбрасывает исключение.
        """
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                options={"verify_exp": True, "verify_iat": False},
            )

            if expected_type and payload.get("type") != expected_type:
                raise InvalidTokenError("Invalid token type")

            return payload

        except ExpiredSignatureError:
            raise ValueError("Token has expired")
        except InvalidTokenError as e:
            raise ValueError(f"Invalid token: {str(e)}")
        except Exception as e:
            raise ValueError(f"Token validation failed: {str(e)}")

    def get_user_id_from_token(self, token: str) -> Optional[int]:
        """Быстрое извлечение user id без полной валидации (для лёгких проверок)."""
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
            return int(payload.get("sub"))
        except:
            return None

    # Пример использования в LoginUC / RefreshUC
    def issue_token_pair(self, user: AppUser | AppUserDTO) -> tuple[str, str]:
        """Выдаёт новую пару токенов (access + refresh с ротацией)."""
        access = self.create_access_token(user)
        refresh = self.create_refresh_token(user)
        return access, refresh

    def build_refresh_token_entity(self, user: AppUser | AppUserDTO, token: str) -> RefreshToken:
        now = datetime.now()
        return RefreshToken(
            user_id=user.id,
            token=token,
            expires_at=now + timedelta(days=self.refresh_token_expire_days),
            created_at=now,
        )

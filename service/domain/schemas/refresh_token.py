# service/domain/schemas/refresh_token.py

from datetime import datetime
from pydantic import BaseModel


class RefreshTokenPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, RefreshTokenPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class RefreshToken(RefreshTokenPK):
    user_id: int
    token: str
    expires_at: datetime
    created_at: datetime

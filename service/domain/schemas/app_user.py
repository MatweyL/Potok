from datetime import datetime
from typing import Set, List

from pydantic import BaseModel

from service.domain.schemas.enums import AppUserRole


class AppUserPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, AppUserPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class AppUser(AppUserPK):
    roles: List[AppUserRole]
    username: str
    password_hash: str
    created_at: datetime


class AppUserDTO(BaseModel):
    username: str
    roles: List[AppUserRole]
    created_at: datetime

    @classmethod
    def from_app_user(cls, app_user: AppUser):
        return cls(username=app_user.username,
                   roles=app_user.roles,
                   created_at=app_user.created_at)
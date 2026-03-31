from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.app_user import AppUserPK, AppUser
from service.domain.schemas.enums import AppUserRole


class SAAppUserRepo(AbstractSARepo):
    def to_model(self, obj: AppUser) -> models.AppUser:
        return models.AppUser(id=obj.id,
                              roles=obj.roles,
                              username=obj.username,
                              password_hash=obj.password_hash,
                              created_at=obj.created_at)

    def to_domain(self, obj: models.AppUser) -> AppUser:
        return AppUser(id=obj.id,
                       roles=[AppUserRole(role_raw) for role_raw in obj.roles],
                       username=obj.username,
                       password_hash=obj.password_hash,
                       created_at=obj.created_at)

    def pk_to_model_pk(self, pk: AppUserPK) -> Dict:
        return {'id': pk.id}

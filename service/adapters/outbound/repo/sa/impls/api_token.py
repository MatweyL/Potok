from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.api_token import ApiTokenPK, ApiToken


class SAApiTokenRepo(AbstractSARepo):
    def to_model(self, obj: ApiToken) -> models.ApiToken:
        return models.ApiToken(id=obj.id,
                               name=obj.name,
                               key_prefix=obj.key_prefix,
                               key_hash=obj.key_hash,
                               user_id=obj.user_id,
                               is_active=obj.is_active,
                               created_at=obj.created_at,
                               last_used_at=obj.last_used_at,
                               expires_at=obj.expires_at)

    def to_domain(self, obj: models.ApiToken) -> ApiToken:
        return ApiToken(id=obj.id,
                        name=obj.name,
                        key_prefix=obj.key_prefix,
                        key_hash=obj.key_hash,
                        user_id=obj.user_id,
                        is_active=obj.is_active,
                        created_at=obj.created_at,
                        last_used_at=obj.last_used_at,
                        expires_at=obj.expires_at)

    def pk_to_model_pk(self, pk: ApiTokenPK) -> Dict:
        return {'id': pk.id}

from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.refresh_token import RefreshToken, RefreshTokenPK


class SARefreshTokenRepo(AbstractSARepo):
    def to_domain(self, obj: models.RefreshToken) -> RefreshToken:
        return RefreshToken(id=obj.id,
                            user_id=obj.user_id,
                            token=obj.token,
                            expires_at=obj.expires_at,
                            created_at=obj.created_at)

    def to_model(self, obj: RefreshToken) -> models.RefreshToken:
        return models.RefreshToken(id=obj.id,
                                   user_id=obj.user_id,
                                   token=obj.token,
                                   expires_at=obj.expires_at,
                                   created_at=obj.created_at)

    def pk_to_model_pk(self, pk: RefreshTokenPK) -> Dict:
        return {
            'id': pk.id
        }

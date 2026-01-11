from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.payload import Payload, PayloadPK


class SAPayloadRepo(AbstractSARepo):
    def to_model(self, obj: Payload) -> models.Payload:
        return models.Payload(id=obj.id,
                              data=obj.data)

    def to_domain(self, obj: models.Payload) -> Payload:
        return Payload(id=obj.id,
                       data=obj.data)

    def pk_to_model_pk(self, pk: PayloadPK) -> Dict:
        return {"id": pk.id}

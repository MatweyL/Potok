from typing import Dict

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.project import Project, ProjectPK


class SAProjectRepo(AbstractSARepo):
    def to_model(self, obj: Project) -> models.Project:
        return models.Project(id=obj.id,
                              title=obj.title,
                              description=obj.description)

    def to_domain(self, obj: models.Project) -> Project:
        return Project(id=obj.id,
                       title=obj.title,
                       description=obj.description)

    def pk_to_model_pk(self, pk: ProjectPK) -> Dict:
        return {"id": pk.id}

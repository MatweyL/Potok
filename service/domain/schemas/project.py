import json

from pydantic import BaseModel


class ProjectPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, ProjectPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Project(ProjectPK):
    title: str
    description: str

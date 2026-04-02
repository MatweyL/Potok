from pydantic import BaseModel


class TaskGroupByProjectPK(BaseModel):
    group_id: int
    project_id: int

    def __eq__(self, other):
        return isinstance(other, TaskGroupByProjectPK) \
               and self.group_id == other.group_id \
               and self.project_id == other.project_id

    def __hash__(self):
        return hash((self.group_id, self.project_id))


class TaskGroupByProject(TaskGroupByProjectPK):
    pass

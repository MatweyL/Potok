# service/domain/use_cases/project_uc.py

from abc import ABC
from typing import Optional, List

from service.domain.schemas.project import Project, ProjectPK
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_group_by_project import TaskGroupByProject, TaskGroupByProjectPK
from service.domain.schemas.task_run_metrics import TaskRunAvgMetrics, TaskRunMetrics
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.task_group import GetAllTaskGroupUC, GetAllTaskGroupUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, UpdateFields, ConditionOperation


# ── Базовый класс ─────────────────────────────────────────────────────────────

class ProjectUC(UseCase, ABC):
    def __init__(
            self,
            project_repo: Repo[Project, Project, ProjectPK],
    ):
        self._project_repo = project_repo


# ══════════════════════════════════════════════════════════════════════════════
# 1. Создание проекта
# ══════════════════════════════════════════════════════════════════════════════

class CreateProjectUCRq(UCRequest):
    title: str
    description: str


class CreateProjectUCRs(UCResponse):
    request: CreateProjectUCRq
    project: Optional[Project] = None


class CreateProjectUC(ProjectUC):
    async def apply(self, request: CreateProjectUCRq) -> CreateProjectUCRs:
        project = Project(title=request.title, description=request.description)
        project = await self._project_repo.create(project)
        return CreateProjectUCRs(success=True, request=request, project=project)


# ══════════════════════════════════════════════════════════════════════════════
# 2. Обновление проекта
# ══════════════════════════════════════════════════════════════════════════════

class UpdateProjectUCRq(UCRequest):
    project_id: int
    title: Optional[str] = None
    description: Optional[str] = None


class UpdateProjectUCRs(UCResponse):
    request: UpdateProjectUCRq
    project: Optional[Project] = None


class UpdateProjectUC(ProjectUC):
    async def apply(self, request: UpdateProjectUCRq) -> UpdateProjectUCRs:
        existing = await self._project_repo.get(ProjectPK(id=request.project_id))
        if not existing:
            return UpdateProjectUCRs(
                success=False, error="Project not found", request=request
            )

        updates = {}
        if request.title is not None:
            updates['title'] = request.title
        if request.description is not None:
            updates['description'] = request.description

        if not updates:
            return UpdateProjectUCRs(success=True, request=request, project=existing)

        updated = await self._project_repo.update(
            ProjectPK(id=request.project_id),
            UpdateFields.multiple(updates)
        )
        return UpdateProjectUCRs(success=True, request=request, project=updated)


# ══════════════════════════════════════════════════════════════════════════════
# Базовый класс для UC работающих со связкой проект-группа
# ══════════════════════════════════════════════════════════════════════════════

class ProjectTaskGroupUC(UseCase, ABC):
    def __init__(
            self,
            project_repo: Repo[Project, Project, ProjectPK],
            task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
            task_group_by_project_repo: Repo[
                TaskGroupByProject, TaskGroupByProject, TaskGroupByProjectPK
            ],
    ):
        self._project_repo = project_repo
        self._task_group_repo = task_group_repo
        self._task_group_by_project_repo = task_group_by_project_repo


# ══════════════════════════════════════════════════════════════════════════════
# 3. Добавление группы задач в проект
# ══════════════════════════════════════════════════════════════════════════════

class AddTaskGroupToProjectUCRq(UCRequest):
    project_id: int
    task_group_id: int


class AddTaskGroupToProjectUCRs(UCResponse):
    request: AddTaskGroupToProjectUCRq
    task_group_by_project: Optional[TaskGroupByProject] = None


class AddTaskGroupToProjectUC(ProjectTaskGroupUC):
    async def apply(self, request: AddTaskGroupToProjectUCRq) -> AddTaskGroupToProjectUCRs:
        # Проект существует?
        project = await self._project_repo.get(ProjectPK(id=request.project_id))
        if not project:
            return AddTaskGroupToProjectUCRs(
                success=False, error="Project not found", request=request
            )

        # Группа существует?
        task_group = await self._task_group_repo.get(TaskGroupPK(id=request.task_group_id))
        if not task_group:
            return AddTaskGroupToProjectUCRs(
                success=False, error="Task group not found", request=request
            )

        # Группа уже привязана к какому-то проекту?
        # У одной группы — только один проект
        existing_links = await self._task_group_by_project_repo.filter(
            FilterFieldsDNF.single('group_id', request.task_group_id)
        )
        if existing_links:
            linked_project_id = existing_links[0].project_id
            return AddTaskGroupToProjectUCRs(
                success=False,
                error=f"Task group already belongs to project #{linked_project_id}",
                request=request,
            )

        link = await self._task_group_by_project_repo.create(
            TaskGroupByProject(
                group_id=request.task_group_id,
                project_id=request.project_id,
            )
        )
        return AddTaskGroupToProjectUCRs(
            success=True, request=request, task_group_by_project=link
        )


# ══════════════════════════════════════════════════════════════════════════════
# 4. Исключение группы задач из проекта
# ══════════════════════════════════════════════════════════════════════════════

class RemoveTaskGroupFromProjectUCRq(UCRequest):
    project_id: int
    task_group_id: int


class RemoveTaskGroupFromProjectUCRs(UCResponse):
    request: RemoveTaskGroupFromProjectUCRq


class RemoveTaskGroupFromProjectUC(ProjectTaskGroupUC):
    async def apply(
            self, request: RemoveTaskGroupFromProjectUCRq
    ) -> RemoveTaskGroupFromProjectUCRs:
        link = await self._task_group_by_project_repo.get(
            TaskGroupByProjectPK(
                group_id=request.task_group_id,
                project_id=request.project_id,
            )
        )
        if not link:
            return RemoveTaskGroupFromProjectUCRs(
                success=False,
                error="Task group is not linked to this project",
                request=request,
            )

        await self._task_group_by_project_repo.delete(link)
        return RemoveTaskGroupFromProjectUCRs(success=True, request=request)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Получение всех групп задач проекта
# ══════════════════════════════════════════════════════════════════════════════

class GetProjectTaskGroupsUCRq(UCRequest):
    project_id: int


class GetProjectTaskGroupsUCRs(UCResponse):
    request: GetProjectTaskGroupsUCRq
    project: Optional[Project] = None
    task_groups: List[TaskGroup] = []

class GetProjectTaskGroupsUC(ProjectTaskGroupUC):
    def __init__(self, project_repo: Repo[Project, Project, ProjectPK],
                 task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK], task_group_by_project_repo: Repo[
                TaskGroupByProject, TaskGroupByProject, TaskGroupByProjectPK
            ],
                 get_all_task_group_uc: GetAllTaskGroupUC):
        super().__init__(project_repo, task_group_repo, task_group_by_project_repo)
        self._get_all_task_group_uc = get_all_task_group_uc

    async def apply(
            self, request: GetProjectTaskGroupsUCRq
    ) -> GetProjectTaskGroupsUCRs:
        project = await self._project_repo.get(ProjectPK(id=request.project_id))
        if not project:
            return GetProjectTaskGroupsUCRs(
                success=False, error="Project not found", request=request
            )

        links = await self._task_group_by_project_repo.filter(
            FilterFieldsDNF.single('project_id', request.project_id)
        )
        if not links:
            return GetProjectTaskGroupsUCRs(
                success=True, request=request, project=project, task_groups=[]
            )

        # Получаем группы по id из связей
        group_ids = [link.group_id for link in links]
        all_task_groups_rs = await self._get_all_task_group_uc.apply(GetAllTaskGroupUCRq(
            filter_fields_dnf=FilterFieldsDNF.single('id', group_ids, operation=ConditionOperation.IN)
        ))
        return GetProjectTaskGroupsUCRs(
            success=True, request=request, project=project, task_groups=all_task_groups_rs.task_groups,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 6. Получение проекта, к которому относится группа
# ══════════════════════════════════════════════════════════════════════════════

class GetTaskGroupProjectUCRq(UCRequest):
    task_group_id: int


class GetTaskGroupProjectUCRs(UCResponse):
    request: GetTaskGroupProjectUCRq
    project: Optional[Project] = None


class GetTaskGroupProjectUC(ProjectTaskGroupUC):
    async def apply(
            self, request: GetTaskGroupProjectUCRq
    ) -> GetTaskGroupProjectUCRs:
        links = await self._task_group_by_project_repo.filter(
            FilterFieldsDNF.single('group_id', request.task_group_id)
        )
        if not links:
            return GetTaskGroupProjectUCRs(
                success=True,
                request=request,
                project=None,  # группа не привязана ни к одному проекту
            )

        project = await self._project_repo.get(ProjectPK(id=links[0].project_id))
        if not project:
            return GetTaskGroupProjectUCRs(
                success=False, error="Project not found", request=request
            )

        return GetTaskGroupProjectUCRs(success=True, request=request, project=project)

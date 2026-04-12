from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, RedirectResponse
from starlette.templating import Jinja2Templates

from service.domain.schemas.enums import AppUserRole, TaskRunStatus
from service.domain.use_cases.external.admin.activate_user import ActivateUserUCRq
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUCRq
from service.domain.use_cases.external.auth.create_user import CreateUserUCRq
from service.domain.use_cases.external.auth.login import LoginUCRq
from service.domain.use_cases.external.auth.logout import LogoutUCRq
from service.domain.use_cases.external.auth.reset_password import ResetPasswordUCRq
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq
from service.domain.use_cases.external.get_task_detailed import GetTaskDetailedUCRq, GetTaskDetailedUCRs
from service.domain.use_cases.external.get_task_group_statistics import GetAllTaskGroupStatisticsUCRq, \
    GetTaskGroupStatisticsUCRq
from service.domain.use_cases.external.get_task_run_detailed import GetTaskRunDetailedUCRs, GetTaskRunDetailedUCRq
from service.domain.use_cases.external.get_task_run_status_logs import GetTaskRunStatusLogsUCRq, \
    GetTaskRunStatusLogsUCRs
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUCRq, GetTaskRunsUCRs
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq, \
    GetMonitoringAlgorithmUCRq
from service.domain.use_cases.external.project import CreateProjectUCRq, GetProjectTaskGroupsUCRq, \
    RemoveTaskGroupFromProjectUCRq, AddTaskGroupToProjectUCRq, UpdateProjectUCRq, GetProjectByTaskGroupUCRq
from service.domain.use_cases.external.task_group import GetAllTaskGroupUCRq, GetTaskGroupUCRq, UpdateTaskGroupUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.domain.use_cases.external.update_task import UpdateTaskUCRq
from service.ports.common.logs import logger
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation

router = APIRouter(tags=["HTML Router"])

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", )


# добавить в router

@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")


@router.post("/auth/login")
async def login(request: Request, rq: LoginUCRq, response: Response):
    rs = await request.app.state.auth_facade.login(rq)
    if rs.success:
        response.set_cookie(
            key="refresh_token",
            value=rs.refresh_token,
            httponly=True,
            secure=True,
            samesite="strict",
            max_age=60 * 60 * 24 * 30,
        )
    return rs


@router.post("/auth/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get("refresh_token")
    await request.app.state.auth_facade.logout(LogoutUCRq(refresh_token=token or ""))

    redirect = RedirectResponse(url="/login", status_code=302)
    redirect.delete_cookie("access_token")
    redirect.delete_cookie("refresh_token")
    return redirect


@router.get("/monitoring-algorithms/json")
async def get_algorithms_json(request: Request):
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms()
    return rs


@router.get("/tasks/create", response_class=HTMLResponse)
def task_create_page(request: Request):
    return templates.TemplateResponse(request=request, name="task_create.html")


@router.post("/tasks/create")
async def create_tasks(request: Request, rq: CreateTasksUCRq):
    rs = await request.app.state.use_case_facade.create_tasks(rq)
    return rs


@router.patch("/payloads/{payload_id}")
async def update_payload(request: Request, payload_id: int, rq: UpdatePayloadUCRq):
    rs = await request.app.state.use_case_facade.update_payload(rq)
    return rs


# -- КОД ВЫШЕ ПОДЛЕЖИТ РЕФАКТОРИНГУ --


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    # текущий юзер уже в request.state после middleware
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    users = []
    if is_admin:
        rs = await request.app.state.admin_use_case_facade.get_all_users()
        users = rs.users

    return templates.TemplateResponse(
        request=request, name="users.html",
        context={"users": users, "is_admin": is_admin}
    )


@router.post("/users")
async def create_user(request: Request, rq: CreateUserUCRq):
    return await request.app.state.auth_facade.create_user(rq)


class ResetPasswordUCRqDTO(BaseModel):
    target_user_id: int
    new_password: str


@router.post("/users/{user_id}/reset-password")
async def reset_password(request: Request, user_id: int, rq: ResetPasswordUCRqDTO):
    current_user = request.state.user
    requestor_roles = current_user.roles
    target_rq = ResetPasswordUCRq(requestor_roles=requestor_roles,
                                  target_user_id=rq.target_user_id,
                                  new_password=rq.new_password)
    return await request.app.state.auth_facade.reset_password(target_rq)


@router.post("/users/{user_id}/deactivate")
async def deactivate_user(request: Request, user_id: int):
    current_user = request.state.user
    return await request.app.state.admin_use_case_facade.deactivate_user(
        DeactivateUserUCRq(target_user_id=user_id,
                           current_user_id=current_user.id)
    )


@router.post("/users/{user_id}/activate")
async def activate_user(request: Request, user_id: int):
    current_user = request.state.user
    return await request.app.state.admin_use_case_facade.activate_user(
        ActivateUserUCRq(target_user_id=user_id,
                         current_user_id=current_user.id)
    )


@router.get("/monitoring-algorithms", response_class=HTMLResponse)
async def monitoring_algorithms_page(request: Request):
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms()
    return templates.TemplateResponse(
        request=request, name="monitoring_algorithms.html",
        context={"algorithms": rs.monitoring_algorithms}
    )


@router.post("/monitoring-algorithms")
async def create_monitoring_algorithm(request: Request, rq: CreateMonitoringAlgorithmUCRq):
    return await request.app.state.use_case_facade.create_monitoring_algorithm(rq)


@router.get("/projects")
async def projects_page(request: Request):
    rs = await request.app.state.use_case_facade.get_all_projects()
    return templates.TemplateResponse(
        request=request, name="projects.html",
        context={"projects": rs.projects}
    )


@router.post("/projects/create")
async def create_project(request: Request, rq: CreateProjectUCRq):
    return await request.app.state.use_case_facade.create_project(rq)


@router.get("/projects/{project_id}/task-groups")
async def get_project_task_groups(request: Request, project_id: int):
    rs = await request.app.state.use_case_facade.get_project_task_groups(
        GetProjectTaskGroupsUCRq(project_id=project_id))
    task_groups = rs.task_groups if rs.task_groups else []
    return task_groups


@router.get("/task-groups/without-project")
async def get_task_groups_without_project(request: Request):
    rs = await request.app.state.use_case_facade.get_task_groups_without_project()
    task_groups = rs.task_groups if rs.task_groups else []
    return task_groups


@router.post("/projects/task-group/bind")
async def add_task_group_to_project(request: Request, rq: AddTaskGroupToProjectUCRq):
    rs = await request.app.state.use_case_facade.add_task_group_to_project(rq)
    return rs


@router.post("/projects/task-group/unbind")
async def remove_task_group_from_project(request: Request, rq: RemoveTaskGroupFromProjectUCRq):
    rs = await request.app.state.use_case_facade.remove_task_group_from_project(rq)
    return rs


@router.get("/projects/{project_id}")
async def project_page(request: Request, project_id: int):
    all_task_group_statistics_rs = await request.app.state.use_case_facade.get_all_task_group_statistics(
        GetAllTaskGroupStatisticsUCRq())
    project_task_groups_rs = await request.app.state.use_case_facade.get_project_task_groups(
        GetProjectTaskGroupsUCRq(project_id=project_id))
    return templates.TemplateResponse(
        request=request, name="project.html",
        context={"task_groups": project_task_groups_rs.task_groups,
                 "project": project_task_groups_rs.project,
                 "task_group_statistics_by_name": all_task_group_statistics_rs.task_group_statistics_by_name}
    )


@router.put("/projects/{project_id}")
async def update_project(request: Request, project_id: int, rq: UpdateProjectUCRq):
    if project_id != rq.project_id:
        raise HTTPException(status_code=400, detail="project_id must be equals in get params and body")
    rs = await request.app.state.use_case_facade.update_project(rq)
    return rs


@router.get("/task-groups")
async def task_groups_page(request: Request):
    all_task_group_statistics_rs = await request.app.state.use_case_facade.get_all_task_group_statistics(
        GetAllTaskGroupStatisticsUCRq())
    task_groups_rs = await request.app.state.use_case_facade.get_all_task_group(GetAllTaskGroupUCRq())
    all_task_group_by_project_detailed_rs = await request.app.state.use_case_facade.get_all_task_group_by_project_detailed()

    return templates.TemplateResponse(
        request=request, name="task_groups.html",
        context={"task_groups": task_groups_rs.task_groups,
                 "task_group_statistics_by_name": all_task_group_statistics_rs.task_group_statistics_by_name,
                 "project_by_task_group_name": all_task_group_by_project_detailed_rs.project_by_task_group_name, }
    )


@router.get("/task-groups/json")
async def task_groups_json(request: Request):
    rs = await request.app.state.use_case_facade.get_all_task_group(GetAllTaskGroupUCRq())
    return rs


@router.get("/task-groups/{task_group_id}")
async def task_group_page(request: Request, task_group_id: int):
    task_group_rs = await request.app.state.use_case_facade.get_task_group(
        GetTaskGroupUCRq(task_group_id=task_group_id))
    task_group_statistics_rs = await request.app.state.use_case_facade.get_task_group_statistics(
        GetTaskGroupStatisticsUCRq(task_group_id=task_group_id))
    project_rs = await request.app.state.use_case_facade.get_project_by_task_group_uc(
        GetProjectByTaskGroupUCRq(task_group_id=task_group_id))
    return templates.TemplateResponse(
        request=request, name="task_group.html",
        context={
            "task_group": task_group_rs.task_group,
            "task_group_statistics": task_group_statistics_rs.task_group_statistics,
            "project": project_rs.project,
        }
    )


@router.put("/task-groups/{task_group_id}")
async def update_group(request: Request, task_group_id: int, rq: UpdateTaskGroupUCRq):
    update_rs = await request.app.state.use_case_facade.update_task_group(rq)
    return update_rs


@router.get("/tasks/")
async def tasks_json(
        request: Request,
        group_id: Optional[int] = None,
        draw: int = 1,
        offset: int = 0,
        limit: int = 25,
        order_by: str = 'id',
        asc_sort: bool = False
):
    pagination = PaginationQuery(
        offset_page=offset,
        limit_per_page=limit,
        order_by=order_by,
        asc_sort=asc_sort,
    )

    rs = await request.app.state.use_case_facade.get_tasks_detailed(
        GetTasksDetailedUCRq(pagination=pagination, task_group_id=group_id)
    )

    return {
        "draw": int(draw),
        "recordsTotal": rs.total,
        "recordsFiltered": rs.total,  # TODO: заменить на total_filtered
        "data": [item.model_dump() for item in rs.tasks],
    }


@router.get("/monitoring-algorithms/{monitoring_algorithm_id}")
async def monitoring_algorithm_page(request: Request, monitoring_algorithm_id: int):
    monitoring_algorithm_rs = await request.app.state.use_case_facade.get_monitoring_algorithm(
        GetMonitoringAlgorithmUCRq(monitoring_algorithm_id=monitoring_algorithm_id))

    return templates.TemplateResponse(
        request=request, name="monitoring_algorithm.html",
        context={
            "algorithm": monitoring_algorithm_rs.monitoring_algorithm
        }
    )


@router.get("/payloads", response_class=HTMLResponse)
async def payloads_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="payloads.html",
    )


@router.get("/payloads/", )
async def payloads_json(request: Request,
                        draw: int = 1,
                        offset: int = 0,
                        limit: int = 25,
                        order_by: str = 'id',
                        asc_sort: bool = False,
                        search_value: Optional[str] = None):
    pagination = PaginationQuery(offset_page=offset,
                                 limit_per_page=limit,
                                 order_by=order_by,
                                 asc_sort=asc_sort
                                 )
    if search_value:
        pagination.filter_fields_dnf = FilterFieldsDNF.single('data', search_value, ConditionOperation.CONTAINS)
    rs = await request.app.state.use_case_facade.get_payloads(
        GetPayloadsUCRq(pagination=pagination)
    )
    return {
        "draw": int(draw),
        "recordsTotal": rs.total,
        "recordsFiltered": rs.total,
        "data": [payload.model_dump() for payload in rs.payloads],
    }


@router.get("/payloads/{payload_id}", response_class=HTMLResponse)
async def payload_page(request: Request, payload_id: int):
    rs = await request.app.state.use_case_facade.get_payload(
        GetPayloadUCRq(payload_id=payload_id, )
    )
    return templates.TemplateResponse(
        request=request, name="payload.html",
        context={
            "payload": rs.payload,
            "tasks_detailed": rs.tasks_detailed_linked,
        }
    )


@router.get("/tasks/{task_id}", response_class=HTMLResponse)
async def task_detailed_page(request: Request, task_id: int, task_run_status: Optional[str] = None):
    task_detailed_rs: GetTaskDetailedUCRs = await request.app.state.use_case_facade.get_task_detailed(
        GetTaskDetailedUCRq(task_id=task_id))
    if not task_detailed_rs.success:
        raise HTTPException(status_code=404, detail=task_detailed_rs.error)
    task_detailed = task_detailed_rs.task_detailed
    return templates.TemplateResponse(
        request=request,
        name="task_detailed.html",
        context={
            "task": task_detailed.task,
            "task_group": task_detailed.task_group,
            "monitoring_algorithm": task_detailed.monitoring_algorithm,
            "payload": task_detailed.payload,
            "progress": task_detailed.progress,
            "runs_status_metrics": task_detailed.runs_status_metrics,
            "task_run_status": task_run_status
        }
    )


@router.patch("/tasks/{task_id}", )
async def update_task(request: Request, task_id: int, rq: UpdateTaskUCRq):
    rs: GetTaskDetailedUCRs = await request.app.state.use_case_facade.update_task(rq)
    return rs


@router.get("/task-runs/")
async def task_runs_json(request: Request,
                      task_id: int,
                      draw: int = 1,
                      offset: int = 0,
                      limit: int = 25,
                      order_by: str = 'id',
                      asc_sort: bool = False,
                      task_run_status: Optional[str] = None
                      ):
    if task_run_status:
        # Если указать TaskRunStatus в запросе, он некорректно конвертируется: сначала преобразуется в перечисление,
        # а затем опять в строку: WAITING (str) -> TaskRunStatus.WAITING (enum) -> TaskRunStatus.WAITING(str)
        try:
            task_run_status = TaskRunStatus(task_run_status[task_run_status.find('.') + 1:].upper())
        except ValueError as e:
            logger.exception(e)
            raise HTTPException(status_code=400, detail=f'unknown status: {task_run_status}')
    pagination = PaginationQuery(offset_page=offset,
                                 limit_per_page=limit,
                                 order_by=order_by,
                                 asc_sort=asc_sort
                                 )
    rs: GetTaskRunsUCRs = await request.app.state.use_case_facade.get_task_runs(GetTaskRunsUCRq(task_id=task_id,
                                                                                                pagination=pagination,
                                                                                                task_run_status=task_run_status))
    return {
        "draw": int(draw),
        "recordsTotal": rs.total,
        "recordsFiltered": rs.total,
        "data": [task_run.model_dump() for task_run in rs.task_runs],
    }


@router.get("/task-runs/{task_run_id}")
async def task_run_detailed_page(request: Request, task_run_id: int):
    rs: GetTaskRunDetailedUCRs = await request.app.state.use_case_facade.get_task_run_detailed(GetTaskRunDetailedUCRq(task_run_id=task_run_id))
    return templates.TemplateResponse(
        request=request,
        name="task_run_detailed.html",
        context={
            "task_run": rs.task_run_detailed.task_run,
            "progress":  rs.task_run_detailed.progress
        }
    )


@router.get("/task-run-status-logs/")
async def task_runs_json(request: Request,
                      task_run_id: int,
                      draw: int = 1,
                      offset: int = 0,
                      limit: int = 25,
                      order_by: str = 'status_updated_at',
                      asc_sort: bool = False,
                      ):

    pagination = PaginationQuery(offset_page=offset,
                                 limit_per_page=limit,
                                 order_by=order_by,
                                 asc_sort=asc_sort
                                 )
    rs: GetTaskRunStatusLogsUCRs = await request.app.state.use_case_facade.get_task_run_status_logs(
        GetTaskRunStatusLogsUCRq(task_run_id=task_run_id,
                                                                                                pagination=pagination)
    )
    return {
        "draw": int(draw),
        "recordsTotal": rs.total,
        "recordsFiltered": rs.total,
        "data": [task_run_status_log.model_dump() for task_run_status_log in rs.task_run_status_logs],
    }

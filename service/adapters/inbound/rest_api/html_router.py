import json

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, RedirectResponse
from starlette.templating import Jinja2Templates

from service.domain.schemas.enums import AppUserRole
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.use_cases.external.admin.activate_user import ActivateUserUCRq
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUCRq
from service.domain.use_cases.external.auth.create_user import CreateUserUCRq
from service.domain.use_cases.external.auth.login import LoginUCRq
from service.domain.use_cases.external.auth.logout import LogoutUCRq
from service.domain.use_cases.external.auth.reset_password import ResetPasswordUCRq
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq
from service.domain.use_cases.external.get_task import GetTaskUCRq
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUCRq
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUCRq
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq
from service.domain.use_cases.external.project import CreateProjectUCRq, GetProjectTaskGroupsUCRq, \
    RemoveTaskGroupFromProjectUCRq, AddTaskGroupToProjectUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF

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


@router.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    rs = await request.app.state.use_case_facade.get_tasks_detailed(
        GetTasksDetailedUCRq(pagination=PaginationQuery())
    )
    return templates.TemplateResponse(
        request=request,
        name="tasks.html",
        context={"tasks": rs.tasks}
    )


# DataTables присылает: draw, start, length, order[0][column], order[0][dir], search[value]
COLUMN_MAP = {
    '0': 'id',
    '1': 'group_name',
    '2': 'status',
    '3': 'priority',
    '4': 'type',
    '5': 'status_updated_at',
}


@router.get("/tasks/json")
async def tasks_json(
        request: Request,
        draw: int = 1,
        start: int = 0,
        length: int = 25,
        # DataTables передаёт order и search через QueryString
):
    params = dict(request.query_params)

    order_col = params.get('order[0][column]', '0')
    order_dir = params.get('order[0][dir]', 'desc')
    search_val = params.get('search[value]', '').strip()

    order_by = COLUMN_MAP.get(order_col, 'id')
    asc_sort = order_dir == 'asc'
    offset_page = start // length

    # TODO: Фильтрация по поиску — ищем по group_name
    filter_dnf = FilterFieldsDNF.empty()

    pagination = PaginationQuery(
        offset_page=offset_page,
        limit_per_page=length,
        order_by=order_by,
        asc_sort=asc_sort,
        filter_fields_dnf=filter_dnf,
    )

    rs = await request.app.state.use_case_facade.get_tasks_detailed(
        GetTasksDetailedUCRq(pagination=pagination)
    )

    # DataTables ожидает recordsTotal и recordsFiltered
    # нужен total count — добавь в GetTasksDetailedUCRs поле total: int
    return {
        "draw": draw,
        "recordsTotal": rs.total,
        "recordsFiltered": rs.total,
        "data": [format_task_row(item) for item in rs.tasks],
    }


def format_task_row(item: TaskDetailed) -> dict:
    task = item.task
    algo = item.monitoring_algorithm
    payload = item.payload

    algo_html = '—'
    if algo:
        algo_html = f'<code class="text-muted small">#{algo.id}</code> {algo.type.title()}'
        if hasattr(algo, 'timeout'):
            noise = f' ± {algo.timeout_noize}с' if algo.timeout_noize else ''
            algo_html += f'<div class="text-muted small">{algo.timeout}с{noise}</div>'

    payload_html = '—'
    if payload and payload.data:
        payload_html = f'<code class="small">{json.dumps(payload.data, ensure_ascii=False)}</code>'

    status_classes = {
        'NEW': 'e0e7ff" style="color:#4338ca',
        'EXECUTION': 'fef3c7" style="color:#b45309',
        'SUCCEED': 'dcfce7" style="color:#15803d',
        'FINISHED': 'f1f5f9" style="color:#64748b',
        'CANCELLED': 'fee2e2" style="color:#b91c1c',
        'ERROR': 'fee2e2" style="color:#991b1b',
    }
    bg = status_classes.get(task.status.value, 'f1f5f9" style="color:#64748b')
    status_html = f'<span style="background:#{bg};font-size:.75rem;font-weight:500;padding:.2rem .6rem;border-radius:20px">{task.status.value.title()}</span>'

    return {
        "DT_RowAttr": {"data-href": f"/tasks/{task.id}", "style": "cursor:pointer"},
        "id": f'<span class="text-muted font-monospace">#{task.id}</span>',
        "group_id": task.group_id,
        "status": status_html,
        "priority": task.priority.value.title(),
        "type": f'<code>{task.type.value}</code>',
        "algorithm": algo_html,
        "payload": payload_html,
        "updated_at": task.status_updated_at.strftime('%d.%m.%Y %H:%M'),
    }


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


@router.get("/tasks/{task_id}", response_class=HTMLResponse)
async def task_detail_page(request: Request, task_id: int):
    task_rs = await request.app.state.use_case_facade.get_task(GetTaskUCRq(task_id=task_id))
    if not task_rs.success:
        raise HTTPException(status_code=404)

    payload_rs = await request.app.state.use_case_facade.get_payload(
        GetPayloadUCRq(payload_id=task_rs.task.payload_id)
    )
    progress_rs = await request.app.state.use_case_facade.get_task_progress(
        GetTaskProgressUCRq(task_id=task_id)
    )
    runs_rs = await request.app.state.use_case_facade.get_task_runs(
        GetTaskRunsUCRq(task_id=task_id)
    )

    return templates.TemplateResponse(
        request=request,
        name="task_detail.html",
        context={
            "task": task_rs.task,
            "payload": payload_rs.payload,
            "task_progress": progress_rs.task_progress,
            "task_runs": runs_rs.task_runs,
        }
    )


@router.get("/payloads", response_class=HTMLResponse)
async def payloads_page(request: Request):
    rs = await request.app.state.use_case_facade.get_payloads(
        GetPayloadsUCRq(pagination=PaginationQuery())
    )
    return templates.TemplateResponse(
        request=request, name="payloads.html",
        context={"payloads": rs.payloads}
    )


@router.patch("/payloads/{payload_id}")
async def update_payload(request: Request, payload_id: int, rq: UpdatePayloadUCRq):
    rs = await request.app.state.use_case_facade.update_payload(rq)
    return rs


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

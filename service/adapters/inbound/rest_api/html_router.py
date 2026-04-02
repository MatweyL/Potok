from fastapi import APIRouter
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, RedirectResponse
from starlette.templating import Jinja2Templates

from service.domain.schemas.enums import AppUserRole
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
from service.domain.use_cases.external.get_tasks import GetTasksUCRq
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery

router = APIRouter()

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


@router.get("/projects", response_class=HTMLResponse)
def projects_page(request: Request, ):
    return templates.TemplateResponse(request=request, name="tasks.html")


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

@router.get("/monitoring-algorithms")
async def get_algorithms(request: Request):
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
        rs = await request.app.state.use_case_facade.get_all_users()
        users = rs.users

    return templates.TemplateResponse(
        request=request, name="users.html",
        context={"users": users, "is_admin": is_admin}
    )

@router.post("/users")
async def create_user(request: Request, rq: CreateUserUCRq):
    return await request.app.state.use_case_facade.create_user(rq)

@router.post("/users/{user_id}/reset-password")
async def reset_password(request: Request, user_id: int, rq: ResetPasswordUCRq):
    current_user = request.state.user
    rq.requestor_roles = current_user.roles
    return await request.app.state.use_case_facade.reset_password(rq)

@router.post("/users/{user_id}/deactivate")
async def deactivate_user(request: Request, user_id: int):
    return await request.app.state.use_case_facade.deactivate_user(
        DeactivateUserUCRq(target_user_id=user_id)
    )
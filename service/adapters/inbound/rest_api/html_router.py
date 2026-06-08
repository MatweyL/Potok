from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, RedirectResponse
from starlette.templating import Jinja2Templates

from service.domain.schemas.enums import AppUserRole, TaskRunStatus
from service.domain.use_cases.external.admin.activate_user import ActivateUserUCRq
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUCRq
from service.domain.use_cases.external.admin.get_all_users import GetAllUsersUCRq
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




@router.get("/tasks/create", response_class=HTMLResponse)
def task_create_page(request: Request):
    return templates.TemplateResponse(request=request, name="task_create.html")


@router.post("/tasks/create")
async def create_tasks(request: Request, rq: CreateTasksUCRq):
    rs = await request.app.state.use_case_facade.create_tasks(rq)
    return rs


# -- КОД ВЫШЕ ПОДЛЕЖИТ РЕФАКТОРИНГУ --



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

@router.get("/dashboard")
async def dashboard(request: Request):
    analytics = request.app.state.analytical_metrics_service
    summary  = await analytics.get_dashboard_summary()
    statuses = await analytics.get_run_status_distribution()
    heatmap  = await analytics.get_run_heatmap()
    trends   = await analytics.get_performance_trends(period="day")
    duration = await analytics.get_duration_distribution()

    STATUS_COLORS = {
        'SUCCEED': '#16a34a', 'EXECUTION': '#2563eb', 'QUEUED': '#60a5fa',
        'WAITING': '#9ca3af', 'ERROR': '#dc2626', 'TEMP_ERROR': '#f59e0b',
        'INTERRUPTED': '#f97316', 'CANCELLED': '#94a3b8',
    }

    # Матрица тепловой карты 7×24 (пн=0)
    matrix = [[0]*24 for _ in range(7)]
    for item in heatmap:
        d = (item.day_of_week - 1) % 7
        matrix[d][item.hour_of_day] = item.run_count

    return templates.TemplateResponse("dashboard.html", {
        "request":         request,
        "summary":         summary,
        "donut_data":      [{"label": s.status, "value": s.run_count,
                             "color": STATUS_COLORS.get(s.status, '#64748b')}
                            for s in statuses],
        "heatmap_matrix":  matrix,
        "histogram_data":  [{"label": d.duration_bucket, "value": d.run_count}
                            for d in duration],
        "histogram_total": sum(d.run_count for d in duration),
        "trend_completed": [{"label": t.period.strftime("%H:%M"),
                             "value": t.completed_count} for t in trends],
        "trend_duration":  [{"label": t.period.strftime("%H:%M"),
                             "value": int(t.avg_duration_seconds or 0)} for t in trends],
    })



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



@router.get("/task-runs/{task_run_id}")
async def task_run_detailed_page(request: Request, task_run_id: int):
    rs: GetTaskRunDetailedUCRs = await request.app.state.use_case_facade.get_task_run_detailed(
        GetTaskRunDetailedUCRq(task_run_id=task_run_id))
    return templates.TemplateResponse(
        request=request,
        name="task_run_detailed.html",
        context={
            "task_run": rs.task_run_detailed.task_run,
            "progress": rs.task_run_detailed.progress
        }
    )


@router.get("/task-run-status-logs")
@router.get("/task-run-status-logs/")
async def task_run_status_logs_json(request: Request,
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

from typing import Optional

from fastapi import APIRouter, Query
from pip._internal import req
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
from service.domain.use_cases.external.task_group import GetAllTaskGroupUCRq, GetTaskGroupUCRq, UpdateTaskGroupUCRq, \
    CreateTaskGroupUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.domain.use_cases.external.update_task import UpdateTaskUCRq
from service.ports.common.logs import logger
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation

router = APIRouter(tags=["Task Groups Router"])
task_groups_router = router
templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))

@router.post('/task-groups')
async def create_task_group(request: Request, rq: CreateTaskGroupUCRq):
    rs = await request.app.state.use_case_facade.create_task_group(rq)
    return rs

@router.get("/task-groups/without-project")
async def get_task_groups_without_project(request: Request):
    rs = await request.app.state.use_case_facade.get_task_groups_without_project()
    task_groups = rs.task_groups if rs.task_groups else []
    return task_groups


@router.get("/task-groups")
async def task_groups_page(request: Request):
    all_task_group_statistics_rs = await request.app.state.use_case_facade.get_all_task_group_statistics(
        GetAllTaskGroupStatisticsUCRq())
    task_groups_rs = await request.app.state.use_case_facade.get_all_task_group(GetAllTaskGroupUCRq())
    all_task_group_by_project_detailed_rs = await request.app.state.use_case_facade.get_all_task_group_by_project_detailed()

    return templates.TemplateResponse(
        request=request, name="task_groups.html",
        context={"groups": task_groups_rs.task_groups,
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

    status_metrics = await request.app.state.analytical_metrics_service.get_run_status_distribution(group_id=task_group_id)
    speed_metrics = await request.app.state.analytical_metrics_service.get_task_group_processing_speed(group_id=task_group_id)
    duration_metrics = await request.app.state.analytical_metrics_service.get_duration_distribution(group_id=task_group_id)
    print(speed_metrics)
    return templates.TemplateResponse(
        request=request, name="task_group_2.html",
        context={
            "task_group": task_group_rs.task_group,
            "task_group_statistics": task_group_statistics_rs.task_group_statistics,
            "project": project_rs.project,
            "status_metrics": status_metrics,
            "speed_metrics": speed_metrics,
            "duration_metrics": duration_metrics,
        }
    )



@router.put("/task-groups/{task_group_id}")
async def update_group(request: Request, task_group_id: int, rq: UpdateTaskGroupUCRq):
    update_rs = await request.app.state.use_case_facade.update_task_group(rq)
    return update_rs

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

router = APIRouter(tags=["Monitoring algos management"])
monitoring_algorithms_router = router

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))

@router.get("/monitoring-algorithms/json")
async def get_algorithms_json(request: Request, page: int = 1, per_page: int = 25, search: str | None = None):
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms()
    return {'items': rs.monitoring_algorithms}


@router.get("/monitoring-algorithms", response_class=HTMLResponse)
async def monitoring_algorithms_page(request: Request):
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms()
    return templates.TemplateResponse(
        request=request, name="monitoring_algorithms.html",
        context={"algorithms": rs.monitoring_algorithms}
    )


@router.post("/monitoring-algorithms")
async def create_monitoring_algorithm(request: Request, rq: CreateMonitoringAlgorithmUCRq):
    logger.warning(rq)
    return await request.app.state.use_case_facade.create_monitoring_algorithm(rq)

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



from typing import Optional, Literal

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

router = APIRouter(tags=["Task Router"])
tasks_router = router
templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/tasks/json")
async def tasks_json(
        request: Request, group_id: int, page: int = 1, per_page: int = 25, search: str | None = None,
        order: Literal["asc", "desc"] = "desc"
):
    if search:
        filter_fields_dnf = FilterFieldsDNF.single('data', search, ConditionOperation.CONTAINS)
    else:
        filter_fields_dnf = FilterFieldsDNF.empty()
    pagination = PaginationQuery(
        offset_page=per_page * (max(page - 1, 0)),
        limit_per_page=per_page,
        order_by='id',
        asc_sort=order == "asc",
        filter_fields_dnf=filter_fields_dnf,
    )
    try:
        rs = await request.app.state.use_case_facade.get_tasks_detailed(
            GetTasksDetailedUCRq(pagination=pagination, task_group_id=group_id)
        )
    except BaseException as e:
        logger.exception(e)
        raise
    return {
        "total": rs.total,
        "items": [item.model_dump() for item in rs.tasks],
    }


@router.get("/task-runs/json")
async def task_runs_json(request: Request,
                         task_id: int,
                         page: int = 1, per_page: int = 25, search: str | None = None,
                         order: Literal["asc", "desc"] = "desc",
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

    pagination = PaginationQuery(
        offset_page=per_page * (max(page - 1, 0)),
        limit_per_page=per_page,
        order_by='id',
        asc_sort=order == "asc",
    )
    rs: GetTaskRunsUCRs = await request.app.state.use_case_facade.get_task_runs(
        GetTaskRunsUCRq(task_id=task_id,
                        pagination=pagination,
                        task_run_status=task_run_status))
    return {
        "total": rs.total,
        "items": [task_run.model_dump() for task_run in rs.task_runs],
    }

@router.get("/task-runs/status-logs/json")
async def task_run_status_logs_json(request: Request,
                         task_run_id: int,
                         page: int = 1,
                         per_page: int = 25,
                         search: str | None = None,
                         order: Literal["asc", "desc"] = "desc"
                         ):
    pagination = PaginationQuery(
        offset_page=per_page * (max(page - 1, 0)),
        limit_per_page=per_page,
        order_by='loaded_at',
        asc_sort=order == "asc",
    )
    rs: GetTaskRunStatusLogsUCRs = await request.app.state.use_case_facade.get_task_run_status_logs(
        GetTaskRunStatusLogsUCRq(task_run_id=task_run_id,
                                 pagination=pagination)
    )
    return {
        "total": rs.total,
        "items": [task_run_status_log.model_dump() for task_run_status_log in rs.task_run_status_logs],
    }
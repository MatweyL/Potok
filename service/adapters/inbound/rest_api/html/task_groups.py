import asyncio

from fastapi import APIRouter
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from service.domain.use_cases.external.get_task_group_statistics import GetAllTaskGroupStatisticsUCRq, \
    GetTaskGroupStatisticsUCRq
from service.domain.use_cases.external.project import GetProjectByTaskGroupUCRq
from service.domain.use_cases.external.task_group import GetAllTaskGroupUCRq, GetTaskGroupUCRq, UpdateTaskGroupUCRq, \
    CreateTaskGroupUCRq
from service.ports.common.path_utils import get_project_root

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
    (
        all_task_group_statistics_rs,
        task_groups_rs,
        all_task_group_by_project_detailed_rs,
    ) = await asyncio.gather(
        request.app.state.use_case_facade.get_all_task_group_statistics(
            GetAllTaskGroupStatisticsUCRq()
        ),
        request.app.state.use_case_facade.get_all_task_group(GetAllTaskGroupUCRq()),
        request.app.state.use_case_facade.get_all_task_group_by_project_detailed(),
    )
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
    (
        task_group_rs,
        task_group_statistics_rs,
        project_rs,
        status_metrics,
        speed_metrics,
        duration_metrics,
    ) = await asyncio.gather(
        request.app.state.use_case_facade.get_task_group(
            GetTaskGroupUCRq(task_group_id=task_group_id)
        ),
        request.app.state.use_case_facade.get_task_group_statistics(
            GetTaskGroupStatisticsUCRq(task_group_id=task_group_id)
        ),
        request.app.state.use_case_facade.get_project_by_task_group_uc(
            GetProjectByTaskGroupUCRq(task_group_id=task_group_id)
        ),
        request.app.state.analytical_metrics_service.get_run_status_distribution(
            group_id=task_group_id
        ),
        request.app.state.analytical_metrics_service.get_task_group_processing_speed(
            group_id=task_group_id
        ),
        request.app.state.analytical_metrics_service.get_duration_distribution(
            group_id=task_group_id
        ),
    )
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

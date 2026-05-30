from typing import Optional

from fastapi import APIRouter, Query
from starlette.requests import Request

analytical_metrics_router = APIRouter(tags=["Analytical Metrics HTML"])


@analytical_metrics_router.get("/api/dashboard/summary")
async def dashboard_summary(request: Request):
    return await request.app.state.analytical_metrics_service.get_dashboard_summary()


@analytical_metrics_router.get("/api/dashboard/run-statuses")
async def dashboard_run_statuses(request: Request, group_id: Optional[int] = None):
    return await request.app.state.analytical_metrics_service.get_run_status_distribution(group_id=group_id)


@analytical_metrics_router.get("/api/dashboard/performance-trends")
async def dashboard_performance_trends(request: Request, period: str = Query("day", pattern="^(day|week)$")):
    return await request.app.state.analytical_metrics_service.get_performance_trends(period=period)


@analytical_metrics_router.get("/api/dashboard/run-heatmap")
async def dashboard_run_heatmap(request: Request):
    return await request.app.state.analytical_metrics_service.get_run_heatmap()


@analytical_metrics_router.get("/api/dashboard/duration-distribution")
async def dashboard_duration_distribution(request: Request, group_id: Optional[int] = None):
    return await request.app.state.analytical_metrics_service.get_duration_distribution(group_id=group_id)


@analytical_metrics_router.get("/api/task-groups/{task_group_id}/processing-speed")
async def task_group_processing_speed(request: Request, task_group_id: int):
    return await request.app.state.analytical_metrics_service.get_task_group_processing_speed(group_id=task_group_id)


@analytical_metrics_router.get("/api/task-groups/{task_group_id}/run-statuses")
async def task_group_run_statuses(request: Request, task_group_id: int):
    return await request.app.state.analytical_metrics_service.get_run_status_distribution(group_id=task_group_id)


@analytical_metrics_router.get("/api/task-groups/{task_group_id}/duration-distribution")
async def task_group_duration_distribution(request: Request, task_group_id: int):
    return await request.app.state.analytical_metrics_service.get_duration_distribution(group_id=task_group_id)


@analytical_metrics_router.get("/api/tasks/{task_id}/run-statistics")
async def task_run_statistics(request: Request, task_id: int):
    return await request.app.state.analytical_metrics_service.get_task_run_statistics(task_id=task_id)

@analytical_metrics_router.get("/api/task-groups")
async def task_run_statistics(request: Request):
    return await request.app.state.analytical_metrics_service.get_groups_statistics()

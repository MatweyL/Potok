from fastapi import APIRouter, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from typing import Optional

from service.di import get_use_case_facade
from service.domain.schemas.enums import PriorityType, TaskType
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.get_task import GetTaskUCRq
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUCRq
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUCRq
from service.domain.use_cases.external.get_tasks import GetTasksUCRq
from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq
from service.domain.schemas.payload import PayloadBody
from service.domain.schemas.task import TaskConfiguration
from service.domain.schemas.monitoring_algorithm import PeriodicMonitoringAlgorithm, SingleMonitoringAlgorithm
from service.ports.outbound.repo.fields import PaginationQuery

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# Home & Dashboard
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Landing page with navigation."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard overview."""
    # Get summary stats
    tasks_response = await get_use_case_facade().get_tasks(
        GetTasksUCRq(pagination=PaginationQuery(limit_per_page=5))
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "total_tasks": len(tasks_response.tasks) if tasks_response.tasks else 0,
        }
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@router.get("/tasks", response_class=HTMLResponse)
async def tasks_page(
        request: Request,
        offset_page: Optional[int] = None,
        limit_per_page: Optional[int] = 20,
        order_by: Optional[str] = None,
        asc_sort: Optional[bool] = None,
):
    """Tasks list page."""
    response = await get_use_case_facade().get_tasks(
        GetTasksUCRq(
            pagination=PaginationQuery(
                offset_page=offset_page,
                limit_per_page=limit_per_page,
                order_by=order_by,
                asc_sort=asc_sort,
            )
        )
    )

    # Get monitoring algorithms for the create form
    algorithms_response = await get_use_case_facade().get_all_monitoring_algorithms()

    return templates.TemplateResponse(
        "tasks/list.html",
        {
            "request": request,
            "tasks": response.tasks or [],
            "algorithms": algorithms_response.algorithms or [],
        }
    )


@router.get("/tasks/{task_id}", response_class=HTMLResponse)
async def task_detail_page(request: Request, task_id: int):
    """Task detail page."""
    task_response = await get_use_case_facade().get_task(GetTaskUCRq(task_id=task_id))
    runs_response = await get_use_case_facade().get_task_runs(GetTaskRunsUCRq(task_id=task_id))
    progress_response = await get_use_case_facade().get_task_progress(GetTaskProgressUCRq(task_id=task_id))

    return templates.TemplateResponse(
        "tasks/detail.html",
        {
            "request": request,
            "task": task_response.task,
            "runs": runs_response.task_runs or [],
            "progress": progress_response.task_progress or [],
        }
    )


@router.get("/tasks/create/modal", response_class=HTMLResponse)
async def create_task_modal(request: Request):
    """Modal for creating a new task."""
    algorithms_response = await get_use_case_facade().get_all_monitoring_algorithms()

    return templates.TemplateResponse(
        "tasks/create_modal.html",
        {
            "request": request,
            "algorithms": algorithms_response.algorithms or [],
        }
    )


@router.post("/tasks/create", response_class=HTMLResponse)
async def create_task_submit(
        request: Request,
        group_name: str = Form(...),
        monitoring_algorithm_id: int = Form(...),
        priority: PriorityType = Form("MEDIUM"),
        task_type: TaskType = Form("UNDEFINED"),
        payload_data: str = Form("{}"),
):
    """Handle task creation form submission."""
    import json

    # Parse payload data
    try:
        data = json.loads(payload_data)
    except:
        data = {}

    # Create task
    task_config = TaskConfiguration(
        group_name=group_name,
        monitoring_algorithm_id=monitoring_algorithm_id,
        priority=priority,
        type=task_type,
    )

    response = await get_use_case_facade().create_tasks(
        CreateTasksUCRq(
            payloads=[PayloadBody(data=data)],
            task_configuration=task_config,
        )
    )

    if response.success and response.tasks:
        # Return HTML row for the new task
        task = response.tasks[0]
        return templates.TemplateResponse(
            "tasks/task_row.html",
            {"request": request, "task": task}
        )

    return HTMLResponse("<div class='alert alert-error'>Failed to create task</div>")


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------

@router.get("/payloads", response_class=HTMLResponse)
async def payloads_page(
        request: Request,
        offset_page: Optional[int] = None,
        limit_per_page: Optional[int] = 20,
        order_by: Optional[str] = None,
        asc_sort: Optional[bool] = None,
):
    """Payloads list page."""
    response = await get_use_case_facade().get_payloads(
        GetPayloadsUCRq(
            pagination=PaginationQuery(
                offset_page=offset_page,
                limit_per_page=limit_per_page,
                order_by=order_by,
                asc_sort=asc_sort,
            )
        )
    )

    return templates.TemplateResponse(
        "payloads/list.html",
        {
            "request": request,
            "payloads": response.payloads or [],
        }
    )


@router.get("/payloads/{payload_id}", response_class=HTMLResponse)
async def payload_detail_page(request: Request, payload_id: int):
    """Payload detail page."""
    response = await get_use_case_facade().get_payload(GetPayloadUCRq(payload_id=payload_id))

    return templates.TemplateResponse(
        "payloads/detail.html",
        {
            "request": request,
            "payload": response.payload,
        }
    )


# ---------------------------------------------------------------------------
# Monitoring Algorithms
# ---------------------------------------------------------------------------

@router.get("/monitoring-algorithms", response_class=HTMLResponse)
async def monitoring_algorithms_page(request: Request):
    """Monitoring algorithms list page."""
    response = await get_use_case_facade().get_all_monitoring_algorithms()

    return templates.TemplateResponse(
        "monitoring_algorithms/list.html",
        {
            "request": request,
            "algorithms": response.algorithms or [],
        }
    )


@router.get("/monitoring-algorithms/create/modal", response_class=HTMLResponse)
async def create_monitoring_algorithm_modal(request: Request):
    """Modal for creating a new monitoring algorithm."""
    return templates.TemplateResponse(
        "monitoring_algorithms/create_modal.html",
        {"request": request}
    )


@router.post("/monitoring-algorithms/create", response_class=HTMLResponse)
async def create_monitoring_algorithm_submit(
        request: Request,
        algorithm_type: str = Form(...),
        timeout: Optional[float] = Form(None),
        timeout_noize: float = Form(0.0),
        timeouts: Optional[str] = Form(None),
):
    """Handle monitoring algorithm creation."""
    import json

    if algorithm_type == "PERIODIC":
        algorithm = PeriodicMonitoringAlgorithm(
            timeout=timeout,
            timeout_noize=timeout_noize,
        )
    elif algorithm_type == "SINGLE":
        timeouts_list = json.loads(timeouts) if timeouts else []
        algorithm = SingleMonitoringAlgorithm(
            timeouts=timeouts_list,
            timeout_noize=timeout_noize,
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Invalid algorithm type</div>")

    response = await get_use_case_facade().create_monitoring_algorithm(
        CreateMonitoringAlgorithmUCRq(algorithm=algorithm)
    )

    if response.success and response.created_algorithm:
        return templates.TemplateResponse(
            "monitoring_algorithms/algorithm_row.html",
            {"request": request, "algorithm": response.created_algorithm}
        )

    return HTMLResponse("<div class='alert alert-error'>Failed to create algorithm</div>")
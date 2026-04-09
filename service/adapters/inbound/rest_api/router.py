from fastapi import APIRouter

from service.di import get_use_case_facade
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq
from service.domain.use_cases.external.get_task import GetTaskUCRq
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUCRq
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUCRq
from service.domain.use_cases.external.get_tasks import GetTasksUCRq
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq
from service.domain.use_cases.external.task_group import CreateTaskGroupUC, CreateTaskGroupUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.ports.outbound.repo.fields import PaginationQuery

router = APIRouter(prefix="/api/v1")


@router.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok"}


@router.post("/tasks")
async def create_tasks(create_tasks_uc_rq: CreateTasksUCRq, ):
    return await get_use_case_facade().create_tasks(create_tasks_uc_rq)


@router.get("/tasks")
async def get_tasks(offset_page: int = None,
                    limit_per_page: int = None,
                    order_by: str = None,
                    asc_sort: bool = None):
    return await get_use_case_facade().get_tasks(GetTasksUCRq(pagination=PaginationQuery(offset_page=offset_page,
                                                                                         limit_per_page=limit_per_page,
                                                                                         order_by=order_by,
                                                                                         asc_sort=asc_sort,
                                                                                         )))


@router.get("/tasks/detailed")
async def get_tasks_detailed(offset_page: int = None,
                             limit_per_page: int = None,
                             order_by: str = None,
                             asc_sort: bool = None):
    return await get_use_case_facade().get_tasks_detailed(
        GetTasksDetailedUCRq(pagination=PaginationQuery(offset_page=offset_page,
                                                        limit_per_page=limit_per_page,
                                                        order_by=order_by,
                                                        asc_sort=asc_sort,
                                                        )))


@router.get("/tasks/{task_id}")
async def get_task(task_id: int):
    return await get_use_case_facade().get_task(GetTaskUCRq(task_id=task_id))


@router.get("/tasks/{task_id}/runs")
async def get_task_runs(task_id: int):
    return await get_use_case_facade().get_task_runs(GetTaskRunsUCRq(task_id=task_id))


@router.get("/tasks/{task_id}/progress")
async def get_task_progress(task_id: int):
    return await get_use_case_facade().get_task_progress(GetTaskProgressUCRq(task_id=task_id))


@router.get("/payloads")
async def get_payloads(offset_page: int = None,
                       limit_per_page: int = None,
                       order_by: str = None,
                       asc_sort: bool = None):
    return await get_use_case_facade().get_payloads(GetPayloadsUCRq(pagination=PaginationQuery(offset_page=offset_page,
                                                                                               limit_per_page=limit_per_page,
                                                                                               order_by=order_by,
                                                                                               asc_sort=asc_sort,
                                                                                               )))


@router.get("/payloads/{payload_id}")
async def get_payload(payload_id: int):
    return await get_use_case_facade().get_payload(GetPayloadUCRq(payload_id=payload_id))


@router.put("/payloads/{payload_id}")
async def update_payload(update_payload_uc_rq: UpdatePayloadUCRq):
    return await get_use_case_facade().update_payload(update_payload_uc_rq)


@router.post("/monitoring-algorithms")
async def create_monitoring_algorithm(create_monitoring_algorithm_uc_rq: CreateMonitoringAlgorithmUCRq):
    return await get_use_case_facade().create_monitoring_algorithm(create_monitoring_algorithm_uc_rq)


@router.get("/monitoring-algorithms")
async def get_all_monitoring_algorithms():
    return await get_use_case_facade().get_all_monitoring_algorithms()


@router.post("/task-group")
async def create_task_group(create_task_group_uc_rq: CreateTaskGroupUCRq):
    return await get_use_case_facade().create_task_group(create_task_group_uc_rq)

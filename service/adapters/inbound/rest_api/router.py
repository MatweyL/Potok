from fastapi import APIRouter

from service.di import get_use_case_facade
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq

router = APIRouter(prefix="/api/v1")


@router.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok"}


@router.post("/tasks")
async def create_tasks(create_tasks_uc_rq: CreateTasksUCRq, ):
    return await get_use_case_facade().create_tasks(create_tasks_uc_rq)


@router.post("/monitoring-algorithms")
async def create_monitoring_algorithm(create_monitoring_algorithm_uc_rq: CreateMonitoringAlgorithmUCRq):
    return await get_use_case_facade().create_monitoring_algorithm(create_monitoring_algorithm_uc_rq)


@router.get("/monitoring-algorithms")
async def get_all_monitoring_algorithms():
    return await get_use_case_facade().get_all_monitoring_algorithms()

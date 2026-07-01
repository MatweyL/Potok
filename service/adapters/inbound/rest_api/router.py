import enum
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from service.di import get_use_case_facade
from service.domain.schemas.enums import PriorityType, TaskType, SimplifiedMonitoringPeriod, TaskStatus
from service.domain.schemas.task import TaskConfiguration
from service.domain.use_cases.external.cancel_tasks import CancelTasksUCRq
from service.domain.use_cases.external.create_payload import CreatePayloadUCRq
from service.domain.use_cases.external.create_tasks import CreateTasksUCRq
from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq, GetPayloadsByGroupUCRq
from service.domain.use_cases.external.get_task_detailed import GetTaskDetailedUCRq
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUCRq
from service.domain.use_cases.external.monitoring_algorithm import FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRq
from service.domain.use_cases.external.resume_tasks import ResumeTasksUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation

router = APIRouter(prefix="/api/v1", tags=["Monitoring API"])


# ── Request / Response schemas ────────────────────────────────────────────────

class CreatePayloadRq(BaseModel):
    data: dict


class CreateMonitoringTaskRq(BaseModel):
    group_id: int
    period: SimplifiedMonitoringPeriod
    priority: PriorityType = PriorityType.MEDIUM


# ── Health ────────────────────────────────────────────────────────────────────

@router.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok"}


# ── Payloads (полезные нагрузки) ─────────────────────────────────────────────────

@router.post(
    "/payloads",
    summary="Создать полезная нагрузка",
    description=(
        "Создаёт новый payload (полезная нагрузка). "
        "Если payload с таким же содержимым `data` уже существует — "
        "возвращает существующий объект без создания дубля."
    ),
)
async def create_payload(
    body: CreatePayloadRq,
):
    facade = get_use_case_facade()
    create_payload_request = CreatePayloadUCRq(data=body.data)
    return await facade.create_payload(create_payload_request)


@router.get(
    "/payloads",
    summary="Список полезных нагрузок",
    description="Возвращает список payload-ов для заданной group_id (платформы).",
)
async def get_payloads(
    group_id: int,
    search: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
):
    facade = get_use_case_facade()
    return await facade.get_payloads_by_group(
        GetPayloadsByGroupUCRq(
            group_id=group_id,
            pagination=PaginationQuery(
                offset_page=per_page * max(page - 1, 0),
                limit_per_page=per_page,
                order_by="id",
                asc_sort=False,
            ),
            search=search,
        )
    )


@router.get(
    "/payloads/{payload_id}",
    summary="Получить полезная нагрузка",
)
async def get_payload(
    payload_id: int,
):
    facade = get_use_case_facade()
    rs = await facade.get_payload(GetPayloadUCRq(payload_id=payload_id, with_tasks=False))
    if not rs.success or not rs.payload:
        raise HTTPException(status_code=404, detail="Payload not found")
    return rs.payload


@router.patch(
    "/payloads/{payload_id}",
    summary="Обновить полезную нагрузку",
    description="Обновляет поле `data` payload-а. Контрольная сумма пересчитывается автоматически.",
)
async def update_payload(
    payload_id: int,
    body: CreatePayloadRq,
    
):
    facade = get_use_case_facade()
    rs = await facade.update_payload(
        UpdatePayloadUCRq(payload_id=payload_id, payload_data=body.data)
    )
    if not rs.success and rs.error and 'Not found' in rs.error:
        raise HTTPException(status_code=404, detail=rs.error or "Payload not found")
    return rs


# ── Tasks (задачи на мониторинг) ──────────────────────────────────────────────

@router.get(
    "/payloads/{payload_id}/tasks",
    summary="Задачи по полезной нагрузке",
    description=(
        "Возвращает список задач мониторинга по данной полезной нагрузке - "
        "на каких платформах мониторится и в каком статусе."
    ),
)
async def get_payload_tasks(
    payload_id: int,
    
):
    facade = get_use_case_facade()
    rs = await facade.get_payload(GetPayloadUCRq(payload_id=payload_id))

    return rs.tasks_detailed_linked

@router.post(
    "/payloads/{payload_id}/tasks",
    summary="Поставить полезная нагрузка на мониторинг",
    description=(
        "Создаёт периодическую задачу мониторинга для указанной полезной нагрузки "
        "на заданной платформе (group_id). "
        "Алгоритм мониторинга подбирается автоматически по period_seconds."
    ),
)
async def create_monitoring_task(
    payload_id: int,
    body: CreateMonitoringTaskRq,
    
):
    facade = get_use_case_facade()

    # Находим или создаём PERIODIC алгоритм с нужным периодом
    algorithm_rs = await facade.find_or_create_simplified_periodic_algorithm(
        FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRq(simplified_monitoring_period=body.period)
    )
    algorithm = algorithm_rs.monitoring_algorithm
    payload_rs = await facade.get_payload(GetPayloadUCRq(payload_id=payload_id, with_tasks=False))
    if not payload_rs.payload:
        raise HTTPException(status_code=400, detail="Payload not found")
    rs = await facade.create_tasks(
        CreateTasksUCRq(
            payloads=[payload_rs.payload],
            task_configuration=TaskConfiguration(
                group_id=body.group_id,
                monitoring_algorithm_id=algorithm.id,
                priority=body.priority,
                type=TaskType.UNDEFINED,
            )
        )
    )
    if not rs.success:
        raise HTTPException(status_code=400, detail=rs.error or "Failed to create task")
    return rs


@router.get(
    "/tasks/{task_id}/detailed",
    summary="Получить статус задачи",
    description=(
        "Возвращает текущий статус и ключевые поля задачи, а так же информацию о "
        "связанных объектах"
    ),
)
async def get_task(
    task_id: int,
    
):
    facade = get_use_case_facade()
    rs = await facade.get_task_detailed(GetTaskDetailedUCRq(task_id=task_id))
    if not rs.success or not rs.task_detailed:
        raise HTTPException(status_code=404, detail="Task not found")
    return rs.task_detailed


@router.post(
    "/tasks/{task_id}/cancel",
    summary="Отменить мониторинг",
    description=(
        "Переводит задачу в статус CANCELLED. "
        "Активные запуски также отменяются. История сборов сохраняется."
    ),
)
async def cancel_task(
    task_id: int,
    
):
    facade = get_use_case_facade()
    rs = await facade.cancel_tasks(CancelTasksUCRq(tasks_ids=[task_id]))
    if not rs.success:
        raise HTTPException(status_code=400, detail=rs.error or "Failed to cancel task")
    if not rs.cancelled_task_by_id or task_id not in rs.cancelled_task_by_id:
        raise HTTPException(
            status_code=409,
            detail="Task cannot be cancelled in its current status",
        )
    return rs


@router.post(
    "/tasks/{task_id}/resume",
    summary="Возобновить мониторинг",
    description=(
        "Возобновляет отменённую задачу. "
        "Задача возвращается в активный статус и снова участвует в очереди сбора."
    ),
)
async def resume_task(
    task_id: int,
    
):
    facade = get_use_case_facade()
    rs = await facade.resume_tasks(ResumeTasksUCRq(tasks_ids=[task_id]))
    if not rs.success:
        raise HTTPException(status_code=400, detail=rs.error or "Failed to resume task")
    if not rs.resumed_task_by_id or task_id not in rs.resumed_task_by_id:
        raise HTTPException(
            status_code=409,
            detail="Task cannot be resumed — it may not be in CANCELLED status",
        )
    return rs
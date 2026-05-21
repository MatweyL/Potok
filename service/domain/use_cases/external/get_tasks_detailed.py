import asyncio
from typing import List, Optional, Dict
from collections import defaultdict

from pydantic import Field

from service.domain.schemas.monitoring_algorithm import MonitoringAlgorithmUnion
from service.domain.schemas.payload import Payload, PayloadPK
from service.domain.schemas.task import TaskPK, Task
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.schemas.task_group import TaskGroup, TaskGroupPK
from service.domain.schemas.task_run import TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_tasks import GetTasksUC, GetTasksUCRq
from service.domain.use_cases.external.monitoring_algorithm import GetAllMonitoringAlgorithmsUC, \
    GetAllMonitoringAlgorithmsUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import (
    PaginationQuery, FilterFieldsDNF, FilterField, ConditionOperation
)
from service.ports.outbound.repo.task_run import TaskRunMetricsProvider, RecentTaskRunsProvider


class GetTasksDetailedUCRq(UCRequest):
    tasks_ids: Optional[List[int]] = None
    task_group_id: Optional[int] = None
    pagination: PaginationQuery
    task_runs_recent_count: int = 3


class GetTasksDetailedUCRs(UCResponse):
    request: GetTasksDetailedUCRq
    tasks: List[TaskDetailed] = Field(default_factory=list)
    total: int
    total_filtered: int

class GetTasksDetailedUC(UseCase):
    def __init__(
            self,
            get_tasks_uc: GetTasksUC,
            get_all_monitoring_algorithms_uc: GetAllMonitoringAlgorithmsUC,
            payload_repo: Repo[Payload, Payload, PayloadPK],
            task_group_repo: Repo[TaskGroup, TaskGroup, TaskGroupPK],
            task_repo: Repo[Task, Task, TaskPK],
            recent_task_runs_provider: RecentTaskRunsProvider,
            task_run_metrics_provider: TaskRunMetricsProvider,
    ):
        self._get_tasks_uc = get_tasks_uc
        self._get_all_monitoring_algorithms_uc = get_all_monitoring_algorithms_uc
        self._payload_repo = payload_repo
        self._task_group_repo = task_group_repo
        self._task_repo = task_repo
        self._recent_task_runs_provider = recent_task_runs_provider
        self._task_run_metrics_provider = task_run_metrics_provider

    async def apply(self, request: GetTasksDetailedUCRq) -> GetTasksDetailedUCRs:
        # ── Фильтры пагинации ────────────────────────────────────────────────
        if request.task_group_id:
            gf = FilterField(name="group_id", value=request.task_group_id)
            if not request.pagination.filter_fields_dnf:
                request.pagination.filter_fields_dnf = FilterFieldsDNF.single("group_id", request.task_group_id)
            else:
                for c in request.pagination.filter_fields_dnf.conjunctions:
                    c.group.append(gf)

        if request.tasks_ids:
            if not request.pagination.filter_fields_dnf:
                request.pagination.filter_fields_dnf = FilterFieldsDNF.single(
                    "id", request.tasks_ids, ConditionOperation.IN
                )
            else:
                for c in request.pagination.filter_fields_dnf.conjunctions:
                    c.group.append(
                        FilterField(name="id", value=request.tasks_ids, operation=ConditionOperation.IN)
                    )

        # ── 1. Задачи ────────────────────────────────────────────────────────
        tasks_rs = await self._get_tasks_uc.apply(GetTasksUCRq(pagination=request.pagination))
        if not tasks_rs.success:
            return GetTasksDetailedUCRs(
                success=False, error=tasks_rs.error,
                request=request, total=0, total_filtered=0,
            )

        tasks = tasks_rs.tasks
        if not tasks:
            return GetTasksDetailedUCRs(success=True, request=request, tasks=[], total=0, total_filtered=0)

        task_ids   = [t.id                          for t in tasks]
        payload_ids = list({t.payload_id            for t in tasks if t.payload_id})
        algo_ids   = list({t.monitoring_algorithm_id for t in tasks if t.monitoring_algorithm_id})
        group_ids  = list({t.group_id               for t in tasks if t.group_id})

        # ── 2. Параллельная загрузка всего ───────────────────────────────────
        (
            payloads_list,
            algorithms_rs,
            groups_list,
            recent_runs,
            metrics_rs,
            total,
            total_filtered,
        ) = await asyncio.gather(
            self._payload_repo.filter(
                FilterFieldsDNF.single("id", payload_ids, ConditionOperation.IN)
            ) if payload_ids else _empty(),

            self._get_all_monitoring_algorithms_uc.apply(
                GetAllMonitoringAlgorithmsUCRq(
                    pagination=PaginationQuery(
                        filter_fields_dnf=FilterFieldsDNF.single("id", algo_ids, ConditionOperation.IN)
                    )
                )
            ) if algo_ids else _empty_algorithms_rs(),

            self._task_group_repo.filter(
                FilterFieldsDNF.single("id", group_ids, ConditionOperation.IN)
            ) if group_ids else _empty(),

            # топ-N runs прямо из БД — без лишних данных в памяти
            self._recent_task_runs_provider.get_recent_per_task(task_ids, request.task_runs_recent_count),

            self._task_run_metrics_provider.provide_tasks_runs_status_metrics(task_ids),

            self._task_repo.count_by_fields(FilterFieldsDNF.empty()),

            self._task_repo.count_by_fields(
                request.pagination.filter_fields_dnf or FilterFieldsDNF.empty()
            ),
        )

        # ── 3. Словари для O(1) доступа ──────────────────────────────────────
        payload_by_id  = {p.id: p for p in payloads_list}
        algo_by_id     = {
            a.id: a
            for a in (algorithms_rs.monitoring_algorithms if algorithms_rs else [])
        }
        group_by_id    = {g.id: g for g in groups_list}

        runs_by_task_id: Dict[int, List[TaskRun]] = defaultdict(list)
        for run in recent_runs:
            runs_by_task_id[run.task_id].append(run)

        metrics_by_task_id = metrics_rs.status_metrics_by_task_id

        # ── 4. Сборка результата ─────────────────────────────────────────────
        result = [
            TaskDetailed(
                task=task,
                payload=payload_by_id.get(task.payload_id),
                monitoring_algorithm=algo_by_id.get(task.monitoring_algorithm_id),
                task_group=group_by_id.get(task.group_id),
                task_runs_recent=runs_by_task_id.get(task.id, []),
                runs_status_metrics=metrics_by_task_id.get(task.id),
            )
            for task in tasks
        ]

        return GetTasksDetailedUCRs(
            success=True, request=request,
            tasks=result, total=total, total_filtered=total_filtered,
        )


# ── Вспомогательные корутины для пустых случаев ──────────────────────────────
async def _empty():
    return []

async def _empty_algorithms_rs():
    return None
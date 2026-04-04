from typing import Optional

from pydantic import Field

from service.domain.schemas.task_run import TaskRunPK, TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo


class GetTaskGroupStatisticsUCRq(UCRequest):
    group_name: str


class GetTaskGroupStatisticsUCRs(UCResponse):
    request: GetTaskGroupStatisticsUCRq
    total_count: int = Field(default=0, description="Всего задач в группе")
    error_count: Optional[int] = Field(default=None, description="Задач, не подлежащих повторным попыткам выполнения")
    queued_count: Optional[int] = Field(default=None, description="Задач в очереди")
    succeed_count: Optional[int] = Field(default=None, description="Выполненных задач")
    waiting_count: Optional[int] = Field(default=None, description="Ожидающих выполнения задач")
    execution_count: Optional[int] = Field(default=None, description="Выполняющихся задач")
    interrupted_count: Optional[int] = Field(default=None, description="Прерванных задач, которые будут выполнены "
                                                                       "повторно позже")
    temp_error_count: Optional[int] = Field(default=None, description="Задач, подлежащих повторным попыткам выполнения")
    cancelled_count: Optional[int] = Field(default=None, description="Отмененных задач, их выполнение можно возобновить")

    avg_queued_duration: float = Field(default=None, description="Среднее время задачи в очереди, сек")
    avg_execution_duration: float = Field(default=None, description="Среднее время выполнения задачи, сек")
    avg_retry_count: float = Field(default=None, description="Среднее количество повторных выполнений задачи. "
                                                             "Фактически, это количество раз появления "
                                                             "статуса WAITING у задачи ")


class GetTaskGroupStatisticsUC(UseCase):
    def __init__(self, task_run_repo: Repo[TaskRun,TaskRun,TaskRunPK]):
        self._task_run_repo = task_run_repo
    async def apply(self, request: GetTaskGroupStatisticsUCRq) -> GetTaskGroupStatisticsUCRs:
        pass

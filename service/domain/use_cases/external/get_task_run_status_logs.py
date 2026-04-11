from typing import Optional, List

from service.domain.schemas.task_run import TaskRun, TaskRunStatusLog, TaskRunPK, TaskRunStatusLogPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF


class GetTaskRunTasksLogsUCRq(UCRequest):
    task_run_id: int
    pagination: Optional[PaginationQuery] = None


class GetTaskRunTasksLogsUCRs(UCResponse):
    request: GetTaskRunTasksLogsUCRq
    task_run: Optional[TaskRun] = None
    task_run_status_logs: Optional[List[TaskRunStatusLog]] = None


class GetTaskRunTasksLogsUC(UseCase):
    def __init__(self, task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK]):
        self._task_run_repo = task_run_repo
        self._task_run_status_log_repo = task_run_status_log_repo

    async def apply(self, request: GetTaskRunTasksLogsUCRq) -> GetTaskRunTasksLogsUCRs:
        task_run = await self._task_run_repo.get(TaskRunPK(id=request.task_run_id))
        if not task_run:
            return GetTaskRunTasksLogsUCRs(success=False, error="Not found", request=request)
        filter_fields_dnf = FilterFieldsDNF.single('task_run_id', request.task_id)
        if request.pagination:
            pagination = request.pagination
            pagination.filter_fields_dnf = filter_fields_dnf
            task_run_status_logs = await self._task_run_status_log_repo.paginated(pagination)
        else:
            task_run_status_logs = await self._task_run_status_log_repo.filter(filter_fields_dnf)
        return GetTaskRunTasksLogsUCRs(success=True,
                                       request=request,
                                       task_run=task_run,
                                       task_run_status_logs=task_run_status_logs)

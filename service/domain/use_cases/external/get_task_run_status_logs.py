from typing import Optional, List

from service.domain.schemas.task_run import TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF


class GetTaskRunStatusLogsUCRq(UCRequest):
    task_run_id: int
    pagination: Optional[PaginationQuery] = None


class GetTaskRunStatusLogsUCRs(UCResponse):
    request: GetTaskRunStatusLogsUCRq
    task_run_status_logs: Optional[List[TaskRunStatusLog]] = None
    total: int= 0


class GetTaskRunStatusLogsUC(UseCase):
    def __init__(self, task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK]):
        self._task_run_status_log_repo = task_run_status_log_repo

    async def apply(self, request: GetTaskRunStatusLogsUCRq) -> GetTaskRunStatusLogsUCRs:
        filter_fields_dnf = FilterFieldsDNF.single('task_run_id', request.task_run_id)
        if request.pagination:
            pagination = request.pagination
            pagination.filter_fields_dnf = filter_fields_dnf
            task_run_status_logs = await self._task_run_status_log_repo.paginated(pagination)
        else:
            task_run_status_logs = await self._task_run_status_log_repo.filter(filter_fields_dnf)
        total = await self._task_run_status_log_repo.count_by_fields(filter_fields_dnf)
        return GetTaskRunStatusLogsUCRs(success=True,
                                        request=request,
                                        task_run_status_logs=task_run_status_logs,
                                        total=total)

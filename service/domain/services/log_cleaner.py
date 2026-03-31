from datetime import datetime, timedelta

from service.domain.schemas.task_run import TaskRunStatusLogPK, TaskRunStatusLog
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


class TaskRunStatusLogCleaner:

    def __init__(self, task_run_status_log: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK],
                 ttl_seconds: int = 86_400):
        self._task_run_status_log = task_run_status_log
        self._ttl_seconds = ttl_seconds

    async def clean_logs(self):
        deleted_logs = await self._task_run_status_log.delete_by_condition(
            FilterFieldsDNF.single('status_updated_at',
                                   datetime.now() - timedelta(seconds=self._ttl_seconds),
                                   ConditionOperation.LT)
        )
        logger.info(f"deleted {deleted_logs} log(s) from {self._task_run_status_log.__class__.__name__}")

from service.domain.schemas.enums import PriorityType, TaskType
from service.domain.schemas.task_run import TaskRunStatusLog, TaskRunStatusLogPK
from service.domain.services.balancing_algorithm.abstract import BalancingAlgorithm
from service.ports.outbound.repo.abstract import Repo


class ConstantBalancingAlgorithm(BalancingAlgorithm):
    def __init__(self, batch_size: int,
                 task_run_status_log_repo: Repo[TaskRunStatusLog, TaskRunStatusLog, TaskRunStatusLogPK]):
        super().__init__(task_run_status_log_repo)
        self._batch_size = batch_size

    async def calculate_batch_size(self, group_name: str, priority_type: PriorityType, task_type: TaskType) -> int:
        return self._batch_size

from service.domain.use_cases.external.create_tasks import CreateTasksUCRq, CreateTasksUCRs, CreateTasksUC
from service.domain.use_cases.external.get_payload import GetPayloadUCRq, GetPayloadUCRs, GetPayloadUC
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq, GetPayloadsUCRs, GetPayloadsUC
from service.domain.use_cases.external.get_task import GetTaskUCRs, GetTaskUCRq, GetTaskUC
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUCRs, GetTaskProgressUCRq, \
    GetTaskProgressUC
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUCRs, GetTaskRunsUCRq, GetTaskRunsUC
from service.domain.use_cases.external.get_tasks import GetTasksUC, GetTasksUCRq, GetTasksUCRs
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUC, GetTasksDetailedUCRs, \
    GetTasksDetailedUCRq
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq, \
    CreateMonitoringAlgorithmUCRs, GetAllMonitoringAlgorithmsUCRq, GetAllMonitoringAlgorithmsUCRs, \
    CreateMonitoringAlgorithmUC, GetAllMonitoringAlgorithmsUC
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq, UpdatePayloadUCRs, UpdatePayloadUC


class UseCaseFacade:
    def __init__(self,
                 create_tasks_uc: CreateTasksUC,
                 create_monitoring_algorithm_uc: CreateMonitoringAlgorithmUC,
                 get_all_monitoring_algorithms_uc: GetAllMonitoringAlgorithmsUC,
                 get_tasks_uc: GetTasksUC,
                 get_task_uc: GetTaskUC,
                 get_task_runs_uc: GetTaskRunsUC,
                 get_task_progress_uc: GetTaskProgressUC,
                 get_payloads_uc: GetPayloadsUC,
                 get_payload_uc: GetPayloadUC,
                 update_payload_uc: UpdatePayloadUC,
                 get_tasks_detailed_uc: GetTasksDetailedUC,
                 ):
        self._create_tasks_uc = create_tasks_uc
        self._create_monitoring_algorithm_uc = create_monitoring_algorithm_uc
        self._get_all_monitoring_algorithms_uc = get_all_monitoring_algorithms_uc
        self._get_tasks_uc = get_tasks_uc
        self._get_task_uc = get_task_uc
        self._get_task_runs_uc = get_task_runs_uc
        self._get_task_progress_uc = get_task_progress_uc
        self._get_payloads_uc = get_payloads_uc
        self._get_payload_uc = get_payload_uc
        self._update_payload_uc = update_payload_uc
        self._get_tasks_detailed_uc = get_tasks_detailed_uc

    async def create_tasks(self, request: CreateTasksUCRq) -> CreateTasksUCRs:
        return await self._create_tasks_uc.apply(request)

    async def get_tasks(self, request: GetTasksUCRq) -> GetTasksUCRs:
        return await self._get_tasks_uc.apply(request)

    async def get_task(self, request: GetTaskUCRq) -> GetTaskUCRs:
        return await self._get_task_uc.apply(request)

    async def get_task_runs(self, request: GetTaskRunsUCRq) -> GetTaskRunsUCRs:
        return await self._get_task_runs_uc.apply(request)

    async def get_task_progress(self, request: GetTaskProgressUCRq) -> GetTaskProgressUCRs:
        return await self._get_task_progress_uc.apply(request)

    async def get_payloads(self, request: GetPayloadsUCRq) -> GetPayloadsUCRs:
        return await self._get_payloads_uc.apply(request)

    async def get_payload(self, request: GetPayloadUCRq) -> GetPayloadUCRs:
        return await self._get_payload_uc.apply(request)

    async def update_payload(self, request: UpdatePayloadUCRq) -> UpdatePayloadUCRs:
        return await self._update_payload_uc.apply(request)

    async def create_monitoring_algorithm(self,
                                          request: CreateMonitoringAlgorithmUCRq) -> CreateMonitoringAlgorithmUCRs:
        return await self._create_monitoring_algorithm_uc.apply(request)

    async def get_all_monitoring_algorithms(self, ) -> GetAllMonitoringAlgorithmsUCRs:
        return await self._get_all_monitoring_algorithms_uc.apply(GetAllMonitoringAlgorithmsUCRq())

    async def get_tasks_detailed(self, request: GetTasksDetailedUCRq) -> GetTasksDetailedUCRs:
        return await self._get_tasks_detailed_uc.apply(request)
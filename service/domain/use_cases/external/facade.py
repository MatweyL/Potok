from service.domain.use_cases.external.create_tasks import CreateTasksUCRq, CreateTasksUCRs, CreateTasksUC
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq, \
    CreateMonitoringAlgorithmUCRs, GetAllMonitoringAlgorithmsUCRq, GetAllMonitoringAlgorithmsUCRs, \
    CreateMonitoringAlgorithmUC, GetAllMonitoringAlgorithmsUC


class UseCaseFacade:
    def __init__(self,
                 create_tasks_uc: CreateTasksUC,
                 create_monitoring_algorithm_uc: CreateMonitoringAlgorithmUC,
                 get_all_monitoring_algorithms_uc_rq: GetAllMonitoringAlgorithmsUC,
                 ):
        self._create_tasks_uc = create_tasks_uc
        self._create_monitoring_algorithm_uc = create_monitoring_algorithm_uc
        self._get_all_monitoring_algorithms_uc_rq = get_all_monitoring_algorithms_uc_rq

    async def create_tasks(self, request: CreateTasksUCRq) -> CreateTasksUCRs:
        return await self._create_tasks_uc.apply(request)

    async def create_monitoring_algorithm(self,
                                          request: CreateMonitoringAlgorithmUCRq) -> CreateMonitoringAlgorithmUCRs:
        return await self._create_monitoring_algorithm_uc.apply(request)

    async def get_all_monitoring_algorithms(self, ) -> GetAllMonitoringAlgorithmsUCRs:
        return await self._get_all_monitoring_algorithms_uc_rq.apply(GetAllMonitoringAlgorithmsUCRq())

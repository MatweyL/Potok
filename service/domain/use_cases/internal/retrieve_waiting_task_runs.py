from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class RetrieveWaitingTaskRunsUCRq(UCRequest):
    pass


class RetrieveWaitingTaskRunsUCRs(UCResponse):
    request: RetrieveWaitingTaskRunsUCRq


class RetrieveWaitingTaskRunsUC(UseCase):
    async def apply(self, request: RetrieveWaitingTaskRunsUCRq) -> RetrieveWaitingTaskRunsUCRs:
        pass

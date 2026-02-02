from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class SendTaskRunsToExecutionUCRq(UCRequest):
    pass


class SendTaskRunsToExecutionUCRs(UCResponse):
    request: SendTaskRunsToExecutionUCRq


class SendTaskRunsToExecutionUC(UseCase):
    async def apply(self, request: SendTaskRunsToExecutionUCRq) -> SendTaskRunsToExecutionUCRs:
        pass

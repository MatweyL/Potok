from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class SendCommandUCRq(UCRequest):
    pass


class SendCommandUCRs(UCResponse):
    request: SendCommandUCRq


class SendCommandUC(UseCase):
    async def apply(self, request: SendCommandUCRq) -> SendCommandUCRs:
        pass

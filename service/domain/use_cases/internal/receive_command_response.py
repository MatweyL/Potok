from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class ReceiveCommandResponseUCRq(UCRequest):
    pass


class ReceiveCommandResponseUCRs(UCResponse):
    request: ReceiveCommandResponseUCRq


class ReceiveCommandResponseUC(UseCase):
    async def apply(self, request: ReceiveCommandResponseUCRq) -> ReceiveCommandResponseUCRs:
        pass

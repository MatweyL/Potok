from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class GetAllUsersUCRq(UCRequest):
    pass


class GetAllUsersUCRs(UCResponse):
    request: GetAllUsersUCRq


class GetAllUsersUC(UseCase):
    async def apply(self, request: GetAllUsersUCRq) -> GetAllUsersUCRs:
        pass

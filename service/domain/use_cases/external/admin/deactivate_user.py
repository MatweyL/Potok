from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class DeactivateUserUCRq(UCRequest):
    target_user_id: int

class DeactivateUserUCRs(UCResponse):
    request: DeactivateUserUCRq


class DeactivateUserUC(UseCase):
    async def apply(self, request: DeactivateUserUCRq) -> DeactivateUserUCRs:
        pass
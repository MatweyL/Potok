from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse


class ResumeTasksUCRq(UCRequest):
    pass


class ResumeTasksUCRs(UCResponse):
    request: ResumeTasksUCRq


class ResumeTasksUC(UseCase):
    async def apply(self, request: ResumeTasksUCRq) -> ResumeTasksUCRs:
        pass

from service.domain.schemas.app_user import AppUser, AppUserPK
from service.domain.schemas.enums import AppUserRole
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.auth.create_user import CreateUserUC, CreateUserUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery


class CreateFirstAdminUCRq(UCRequest):
    username: str
    password: str


class CreateFirstAdminUCRs(UCResponse):
    request: CreateFirstAdminUCRq


class CreateFirstAdminUC(UseCase):
    def __init__(self,
                 create_user: CreateUserUC,
                 app_user_repo: Repo[AppUser, AppUser, AppUserPK], ):
        self._create_user = create_user
        self._app_user_repo = app_user_repo

    async def apply(self, request: CreateFirstAdminUCRq) -> CreateFirstAdminUCRs:
        user = await self._app_user_repo.paginated(PaginationQuery(limit_per_page=1,
                                                                   order_by='created_at',
                                                                   asc_sort=True))
        if not user:
            create_user_rs = await self._create_user.apply(CreateUserUCRq(username=request.username,
                                                                          password=request.password,
                                                                          roles=[AppUserRole.ADMIN]))
            if create_user_rs.error:
                raise RuntimeError(create_user_rs.error)
            return CreateFirstAdminUCRs(success=True, request=request)
        else:
            return CreateFirstAdminUCRs(success=False, error="Admin already created!", request=request)

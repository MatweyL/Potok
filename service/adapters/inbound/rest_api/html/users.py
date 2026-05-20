from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates

from service.domain.schemas.enums import AppUserRole
from service.domain.use_cases.external.admin.activate_user import ActivateUserUCRq
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUCRq
from service.domain.use_cases.external.admin.get_all_users import GetAllUsersUCRq
from service.domain.use_cases.external.auth.create_user import CreateUserUCRq
from service.domain.use_cases.external.auth.reset_password import ResetPasswordUCRq
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation

router = APIRouter(tags=["User management"])
users_router = router

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    # текущий юзер уже в request.state после middleware
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    if is_admin:
        return templates.TemplateResponse(
            request=request, name="users.html",
            context={"is_admin": is_admin}
        )
    else:
        return templates.TemplateResponse(request=request, name="404.html")


@router.get("/users/json")
async def users_json(request: Request, page: int = 1, per_page: int = 25, search: str | None = None):
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    if is_admin:
        if search:
            filter_fields_dnf = FilterFieldsDNF.single('username', search, ConditionOperation.CONTAINS)
        else:
            filter_fields_dnf = FilterFieldsDNF.empty()
        rq = GetAllUsersUCRq(pagination=PaginationQuery(
            offset_page=per_page * (max(page - 1, 0)),
            limit_per_page=per_page,
            order_by='created_at',
            asc_sort=False,
            filter_fields_dnf=filter_fields_dnf,
        ))
        rs = await request.app.state.admin_use_case_facade.get_all_users(rq)
        users = rs.users
        return {'items': users}
    else:
        return HTTPException(status_code=403)


@router.post("/users")
async def create_user(request: Request, rq: CreateUserUCRq):
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles
    
    if is_admin:
        return await request.app.state.auth_facade.create_user(rq)
    return HTTPException(status_code=403)

class ResetPasswordUCRqDTO(BaseModel):
    target_user_id: int
    new_password: str


@router.post("/users/{user_id}/reset-password")
async def reset_password(request: Request, user_id: int, rq: ResetPasswordUCRqDTO):
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    if is_admin:
        requestor_roles = current_user.roles
        target_rq = ResetPasswordUCRq(requestor_roles=requestor_roles,
                                      target_user_id=rq.target_user_id,
                                      new_password=rq.new_password)
        return await request.app.state.auth_facade.reset_password(target_rq)
    return HTTPException(status_code=403)

@router.post("/users/{user_id}/deactivate")
async def deactivate_user(request: Request, user_id: int):
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    if is_admin:
        return await request.app.state.admin_use_case_facade.deactivate_user(
            DeactivateUserUCRq(target_user_id=user_id,
                               current_user_id=current_user.id)
        )
    return HTTPException(status_code=403)

@router.post("/users/{user_id}/activate")
async def activate_user(request: Request, user_id: int):
    current_user = request.state.user
    is_admin = AppUserRole.ADMIN in current_user.roles

    if is_admin:
        return await request.app.state.admin_use_case_facade.activate_user(
            ActivateUserUCRq(target_user_id=user_id,
                             current_user_id=current_user.id)
        )
    return HTTPException(status_code=403)
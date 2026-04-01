from fastapi import APIRouter, Depends
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, RedirectResponse
from starlette.templating import Jinja2Templates

from service.domain.use_cases.external.auth.login import LoginUCRq
from service.domain.use_cases.external.auth.logout import LogoutUCRq
from service.domain.use_cases.external.auth.refresh_token import RefreshTokenUCRq
from service.ports.common.path_utils import get_project_root

router = APIRouter()

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", )


# добавить в router

@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")


@router.post("/auth/login")
async def login(request: Request, rq: LoginUCRq, response: Response):
    rs = await request.app.state.auth_facade.login(rq)
    if rs.success:
        response.set_cookie(
            key="refresh_token",
            value=rs.refresh_token,
            httponly=True,
            secure=True,
            samesite="strict",
            max_age=60 * 60 * 24 * 30,
        )
    return rs


@router.post("/auth/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get("refresh_token")
    await request.app.state.auth_facade.logout(LogoutUCRq(refresh_token=token or ""))

    redirect = RedirectResponse(url="/login", status_code=302)
    redirect.delete_cookie("access_token")
    redirect.delete_cookie("refresh_token")
    return redirect


@router.get("/projects", response_class=HTMLResponse)
def projects_page(request: Request, ):
    return templates.TemplateResponse(request=request, name="projects.html")

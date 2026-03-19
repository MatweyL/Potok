from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates

from service.ports.common.path_utils import get_project_root

router = APIRouter()

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", )

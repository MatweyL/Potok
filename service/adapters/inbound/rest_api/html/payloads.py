from typing import Optional, Literal

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates

from service.domain.use_cases.external.get_payload import GetPayloadUCRq
from service.domain.use_cases.external.get_payloads import GetPayloadsUCRq
from service.domain.use_cases.external.update_payload import UpdatePayloadUCRq
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation

router = APIRouter(tags=["Payload management"])
payloads_router = router

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/payloads", response_class=HTMLResponse)
async def payloads_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="payloads.html",
    )


@router.get("/payloads/json", )
async def get_payloads_json(request: Request, page: int = 1, per_page: int = 25, search: str | None = None,
                              order: Literal["asc", "desc"] = "desc"):
    if search:
        filter_fields_dnf = FilterFieldsDNF.single('data', search, ConditionOperation.CONTAINS)
    else:
        filter_fields_dnf = FilterFieldsDNF.empty()
    pagination = PaginationQuery(
        offset_page=per_page * (max(page - 1, 0)),
        limit_per_page=per_page,
        order_by='id',
        asc_sort=order == "asc",
        filter_fields_dnf=filter_fields_dnf,
    )
    rs = await request.app.state.use_case_facade.get_payloads(
        GetPayloadsUCRq(pagination=pagination)
    )
    return {'items': rs.payloads,
            'total': rs.total}


@router.get("/payloads/{payload_id}", response_class=HTMLResponse)
async def payload_page(request: Request, payload_id: int):
    rs = await request.app.state.use_case_facade.get_payload(
        GetPayloadUCRq(payload_id=payload_id, )
    )
    if not rs.success:
        return templates.TemplateResponse(request=request, name="404.html")
    return templates.TemplateResponse(
        request=request, name="payload.html",
        context={
            "payload": rs.payload,
            "tasks_detailed": rs.tasks_detailed_linked,
        }
    )


@router.patch("/payloads/{payload_id}")
async def update_payload(request: Request, payload_id: int, rq: UpdatePayloadUCRq):
    rs = await request.app.state.use_case_facade.update_payload(rq)
    return rs

from typing import Literal

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates

from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUCRq, \
    GetMonitoringAlgorithmUCRq, GetAllMonitoringAlgorithmsUCRq, UpdateMonitoringAlgorithmUCRq
from service.ports.common.logs import logger
from service.ports.common.path_utils import get_project_root
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation, \
    FilterFieldsConjunct

router = APIRouter(tags=["Monitoring algos management"])
monitoring_algorithms_router = router

templates = Jinja2Templates(directory=get_project_root().joinpath('templates'))


@router.get("/monitoring-algorithms/json")
async def get_algorithms_json(request: Request, page: int = 1, per_page: int = 25, search: str | None = None,
                              order: Literal["asc", "desc"] = "desc"):
    if search:
        filter_fields_dnf = FilterFieldsDNF(conjunctions=[
            FilterFieldsConjunct.single('name', search, ConditionOperation.CONTAINS),
            FilterFieldsConjunct.single('description', search, ConditionOperation.CONTAINS)
        ])
    else:
        filter_fields_dnf = FilterFieldsDNF.empty()
    pagination = PaginationQuery(
        offset_page=per_page * (max(page - 1, 0)),
        limit_per_page=per_page,
        order_by='id',
        asc_sort=order == "asc",
        filter_fields_dnf=filter_fields_dnf,
    )
    rq = GetAllMonitoringAlgorithmsUCRq(pagination=pagination)
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms(rq)
    return {'items': rs.monitoring_algorithms}  # TODO: возвращать total


@router.get("/monitoring-algorithms", response_class=HTMLResponse)
async def monitoring_algorithms_page(request: Request):
    pagination = PaginationQuery(offset_page=0, limit_per_page=25, asc_sort=True, order_by='id')
    rq = GetAllMonitoringAlgorithmsUCRq(pagination=pagination)
    rs = await request.app.state.use_case_facade.get_all_monitoring_algorithms(rq)
    return templates.TemplateResponse(
        request=request, name="monitoring_algorithms.html",
        context={"algorithms": rs.monitoring_algorithms}
    )


@router.post("/monitoring-algorithms")
async def create_monitoring_algorithm(request: Request, rq: CreateMonitoringAlgorithmUCRq):
    return await request.app.state.use_case_facade.create_monitoring_algorithm(rq)


@router.get("/monitoring-algorithms/{monitoring_algorithm_id}")
async def monitoring_algorithm_page(request: Request, monitoring_algorithm_id: int):
    monitoring_algorithm_rs = await request.app.state.use_case_facade.get_monitoring_algorithm(
        GetMonitoringAlgorithmUCRq(monitoring_algorithm_id=monitoring_algorithm_id))
    if not monitoring_algorithm_rs.success:
        return templates.TemplateResponse(request=request, name="404.html", )
    return templates.TemplateResponse(
        request=request, name="monitoring_algorithm.html",
        context={
            "algorithm": monitoring_algorithm_rs.monitoring_algorithm
        }
    )


@router.put("/monitoring-algorithms/{monitoring_algorithm_id}")
async def update_monitoring_algorithm(request: Request, monitoring_algorithm_id: int,
                                      rq: UpdateMonitoringAlgorithmUCRq, ):
    return await request.app.state.use_case_facade.update_monitoring_algorithm(rq)

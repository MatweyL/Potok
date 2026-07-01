from typing import List, Optional

from service.domain.schemas.enums import MonitoringAlgorithmType, SimplifiedMonitoringPeriod
from service.domain.schemas.monitoring_algorithm import (
    MonitoringAlgorithm,
    MonitoringAlgorithmPK,
    PeriodicMonitoringAlgorithm,
    MonitoringAlgorithmUnion, SingleMonitoringAlgorithm,
)
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, ConditionOperation, UpdateFields, \
    FilterField
from service.ports.outbound.repo.transaction import TransactionFactory


# ---------------------------------------------------------------------------
# Create Monitoring Algorithm Use Case
# ---------------------------------------------------------------------------


class CreateMonitoringAlgorithmUCRq(UCRequest):
    algorithm: MonitoringAlgorithmUnion


class CreateMonitoringAlgorithmUCRs(UCResponse):
    request: CreateMonitoringAlgorithmUCRq
    created_algorithm: Optional[MonitoringAlgorithmUnion] = None


class CreateMonitoringAlgorithmUC(UseCase):
    """
    Создаёт новый алгоритм мониторинга.
    Поддерживаемые типы: PERIODIC, SINGLE.
    """

    def __init__(
            self,
            monitoring_algorithm_repo: Repo[
                MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            periodic_monitoring_algorithm_repo: Repo[
                PeriodicMonitoringAlgorithm, PeriodicMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            single_monitoring_algorithm_repo: Repo[
                SingleMonitoringAlgorithm, SingleMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            transaction_factory: TransactionFactory,
    ):
        self._monitoring_algorithm_repo = monitoring_algorithm_repo
        self._periodic_monitoring_algorithm_repo = periodic_monitoring_algorithm_repo
        self._single_monitoring_algorithm_repo = single_monitoring_algorithm_repo
        self._transaction_factory = transaction_factory

    async def apply(
            self, request: CreateMonitoringAlgorithmUCRq
    ) -> CreateMonitoringAlgorithmUCRs:
        try:
            async with self._transaction_factory.create() as transaction:
                created_ma = await self._monitoring_algorithm_repo.create(request.algorithm, transaction)
                request.algorithm.id = created_ma.id
                if request.algorithm.type == MonitoringAlgorithmType.PERIODIC:
                    created = await self._periodic_monitoring_algorithm_repo.create(request.algorithm, transaction)
                elif request.algorithm.type == MonitoringAlgorithmType.SINGLE:
                    created = await self._single_monitoring_algorithm_repo.create(request.algorithm, transaction)
                else:
                    raise ValueError(f"No repository implemented for {request.algorithm.type=}")
        except BaseException as e:
            return CreateMonitoringAlgorithmUCRs(success=False, error=str(e), request=request, created_algorithm=None)
        else:
            created.title = created_ma.title
            created.description = created_ma.description
            return CreateMonitoringAlgorithmUCRs(
                success=True,
                request=request,
                created_algorithm=created,
            )


# ---------------------------------------------------------------------------
# Get All Monitoring Algorithms Use Case
# ---------------------------------------------------------------------------


class GetAllMonitoringAlgorithmsUCRq(UCRequest):
    pagination: PaginationQuery


class GetAllMonitoringAlgorithmsUCRs(UCResponse):
    request: GetAllMonitoringAlgorithmsUCRq
    monitoring_algorithms: List[MonitoringAlgorithmUnion] = []


class GetAllMonitoringAlgorithmsUC(UseCase):
    """
    Возвращает все алгоритмы мониторинга из БД.
    """

    def __init__(
            self,
            monitoring_algorithm_repo: Repo[
                MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            monitoring_algorithm_repos: List[Repo[
                MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
            ],]
    ):
        self._monitoring_algorithm_repo = monitoring_algorithm_repo
        self._monitoring_algorithm_repos = monitoring_algorithm_repos

    async def apply(
            self, request: GetAllMonitoringAlgorithmsUCRq
    ) -> GetAllMonitoringAlgorithmsUCRs:
        algorithms_total = []
        base_algos = await self._monitoring_algorithm_repo.paginated(request.pagination)
        base_algo_by_id = {base_algo.id: base_algo for base_algo in base_algos}
        base_alogs_ids = list(base_algo_by_id.keys())
        for monitoring_algorithm_repo in self._monitoring_algorithm_repos:
            algorithms = await monitoring_algorithm_repo.filter(
                FilterFieldsDNF.single('id', base_alogs_ids, ConditionOperation.IN))
            algorithms_total.extend(algorithms)
        for algorithm in algorithms_total:
            base_algo = base_algo_by_id[algorithm.id]
            algorithm.title = base_algo.title
            algorithm.description = base_algo.description
        sort_direction = 1 if request.pagination.asc_sort else -1
        algorithms_total.sort(key=lambda at: sort_direction * at.id)
        return GetAllMonitoringAlgorithmsUCRs(
            success=True,
            request=request,
            monitoring_algorithms=algorithms_total,
        )


class GetMonitoringAlgorithmUCRq(UCRequest):
    monitoring_algorithm_id: int


class GetMonitoringAlgorithmUCRs(UCResponse):
    request: GetMonitoringAlgorithmUCRq
    monitoring_algorithm: Optional[MonitoringAlgorithmUnion] = None


class GetMonitoringAlgorithmUC(UseCase):
    """
    Возвращает все алгоритмы мониторинга из БД.
    """

    def __init__(
            self,
            monitoring_algorithm_repo: Repo[
                MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            periodic_monitoring_algorithm_repo: Repo[
                PeriodicMonitoringAlgorithm, PeriodicMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            single_monitoring_algorithm_repo: Repo[
                SingleMonitoringAlgorithm, SingleMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            transaction_factory: TransactionFactory,
    ):
        self._monitoring_algorithm_repo = monitoring_algorithm_repo
        self._periodic_monitoring_algorithm_repo = periodic_monitoring_algorithm_repo
        self._single_monitoring_algorithm_repo = single_monitoring_algorithm_repo
        self._transaction_factory = transaction_factory

    async def apply(
            self, request: GetMonitoringAlgorithmUCRq
    ) -> GetMonitoringAlgorithmUCRs:
        monitoring_algorithm_pk = MonitoringAlgorithmPK(id=request.monitoring_algorithm_id)
        base_algorithm = await self._monitoring_algorithm_repo.get(monitoring_algorithm_pk)
        if not base_algorithm:
            return GetMonitoringAlgorithmUCRs(success=False, request=request, error="Monitoring algorithm not found")
        if base_algorithm.type == MonitoringAlgorithmType.PERIODIC:
            monitoring_algorithm = await self._periodic_monitoring_algorithm_repo.get(monitoring_algorithm_pk)
        elif base_algorithm.type == MonitoringAlgorithmType.SINGLE:
            monitoring_algorithm = await self._single_monitoring_algorithm_repo.get(monitoring_algorithm_pk)
        else:
            raise RuntimeError(f"Unknown algorithm: {monitoring_algorithm_pk}")
        monitoring_algorithm.title = base_algorithm.title
        monitoring_algorithm.description = base_algorithm.description
        return GetMonitoringAlgorithmUCRs(success=True, request=request,
                                          monitoring_algorithm=monitoring_algorithm)


class UpdateMonitoringAlgorithmUCRq(UCRequest):
    monitoring_algorithm_id: int
    title: str | None = None
    description: str | None = None


class UpdateMonitoringAlgorithmUCRs(UCResponse):
    request: UpdateMonitoringAlgorithmUCRq
    monitoring_algorithm: Optional[MonitoringAlgorithmUnion] = None


class UpdateMonitoringAlgorithmUC(UseCase):

    def __init__(
            self,
            monitoring_algorithm_repo: Repo[
                MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            periodic_monitoring_algorithm_repo: Repo[
                PeriodicMonitoringAlgorithm, PeriodicMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            single_monitoring_algorithm_repo: Repo[
                SingleMonitoringAlgorithm, SingleMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
            transaction_factory: TransactionFactory,
    ):
        self._monitoring_algorithm_repo = monitoring_algorithm_repo
        self._periodic_monitoring_algorithm_repo = periodic_monitoring_algorithm_repo
        self._single_monitoring_algorithm_repo = single_monitoring_algorithm_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: UpdateMonitoringAlgorithmUCRq) -> UpdateMonitoringAlgorithmUCRs:
        monitoring_algorithm_pk = MonitoringAlgorithmPK(id=request.monitoring_algorithm_id)
        base_algorithm = await self._monitoring_algorithm_repo.update(monitoring_algorithm_pk,
            UpdateFields.multiple({'name': request.title,
                                   'description': request.description}))
        if base_algorithm.type == MonitoringAlgorithmType.PERIODIC:
            monitoring_algorithm = await self._periodic_monitoring_algorithm_repo.get(monitoring_algorithm_pk)
        elif base_algorithm.type == MonitoringAlgorithmType.SINGLE:
            monitoring_algorithm = await self._single_monitoring_algorithm_repo.get(monitoring_algorithm_pk)
        else:
            raise RuntimeError(f"Unknown algorithm: {monitoring_algorithm_pk}")
        monitoring_algorithm.title = base_algorithm.title
        monitoring_algorithm.description = base_algorithm.description
        return UpdateMonitoringAlgorithmUCRs(success=True, request=request, monitoring_algorithm=monitoring_algorithm)


class FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRq(UCRequest):
    simplified_monitoring_period: SimplifiedMonitoringPeriod


class FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRs(UCResponse):
    request: FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRq
    monitoring_algorithm: PeriodicMonitoringAlgorithm


class FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUC(UseCase):
    def __init__(
            self,
            create_monitoring_algorithm_uc: CreateMonitoringAlgorithmUC,
            periodic_monitoring_algorithm_repo: Repo[
                PeriodicMonitoringAlgorithm, PeriodicMonitoringAlgorithm, MonitoringAlgorithmPK
            ],
    ):
        self._create_monitoring_algorithm_uc  = create_monitoring_algorithm_uc
        self._periodic_monitoring_algorithm_repo = periodic_monitoring_algorithm_repo

    async def apply(self, request: FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRq) -> FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRs:
        timeout = period_to_seconds(request.simplified_monitoring_period)
        filter_fields_dnf = FilterFieldsDNF.single_conjunct([FilterField.new('timeout', timeout, ConditionOperation.EQ),
                                                             FilterField.new('timeout_noize', 0, ConditionOperation.EQ)])
        periodic_monitoring_algorithms = await self._periodic_monitoring_algorithm_repo.filter(filter_fields_dnf)
        if periodic_monitoring_algorithms:
            periodic_monitoring_algorithm = periodic_monitoring_algorithms[0]
        else:
            create_rq = CreateMonitoringAlgorithmUCRq(algorithm=PeriodicMonitoringAlgorithm(timeout=timeout))
            create_rs = await self._create_monitoring_algorithm_uc.apply(create_rq)
            periodic_monitoring_algorithm: PeriodicMonitoringAlgorithm = create_rs.created_algorithm
        return FindOrCreateSimplifiedPeriodicMonitoringAlgorithmUCRs(request=request,
                                                                     success=True,
                                                                     monitoring_algorithm=periodic_monitoring_algorithm)



def period_to_seconds(period: SimplifiedMonitoringPeriod) -> int:
    """Convert enum period to seconds."""
    mapping = {
        SimplifiedMonitoringPeriod.THIRTY_MINUTES: 30 * 60,
        SimplifiedMonitoringPeriod.HOUR: 60 * 60,
        SimplifiedMonitoringPeriod.TWO_HOURS: 2 * 60 * 60,
        SimplifiedMonitoringPeriod.FOUR_HOURS: 4 * 60 * 60,
        SimplifiedMonitoringPeriod.EIGHT_HOURS: 8 * 60 * 60,
        SimplifiedMonitoringPeriod.TWELVE_HOURS: 12 * 60 * 60,
        SimplifiedMonitoringPeriod.DAY: 24 * 60 * 60,
        SimplifiedMonitoringPeriod.WEEK: 7 * 24 * 60 * 60,
        SimplifiedMonitoringPeriod.MONTH: 30 * 24 * 60 * 60,  # 30 дней
    }
    return mapping[period]
from typing import List, Optional

from service.domain.schemas.enums import MonitoringAlgorithmType
from service.domain.schemas.monitoring_algorithm import (
    MonitoringAlgorithm,
    MonitoringAlgorithmPK,
    PeriodicMonitoringAlgorithm,
    MonitoringAlgorithmUnion, SingleMonitoringAlgorithm,
)
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
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
            return CreateMonitoringAlgorithmUCRs(
                success=True,
                request=request,
                created_algorithm=created,
            )


# ---------------------------------------------------------------------------
# Get All Monitoring Algorithms Use Case
# ---------------------------------------------------------------------------


class GetAllMonitoringAlgorithmsUCRq(UCRequest):
    pass


class GetAllMonitoringAlgorithmsUCRs(UCResponse):
    request: GetAllMonitoringAlgorithmsUCRq
    algorithms: List[MonitoringAlgorithm] = []


class GetAllMonitoringAlgorithmsUC(UseCase):
    """
    Возвращает все алгоритмы мониторинга из БД.
    """

    def __init__(
            self,
            monitoring_algorithm_repos: List[Repo[
                                                 MonitoringAlgorithm, MonitoringAlgorithm, MonitoringAlgorithmPK
                                             ],]
    ):
        self._monitoring_algorithm_repos = monitoring_algorithm_repos

    async def apply(
            self, request: GetAllMonitoringAlgorithmsUCRq
    ) -> GetAllMonitoringAlgorithmsUCRs:
        algorithms_total = []
        for monitoring_algorithm_repo in self._monitoring_algorithm_repos:
            algorithms = await monitoring_algorithm_repo.get_all()
            algorithms_total.extend(algorithms)
        return GetAllMonitoringAlgorithmsUCRs(
            success=True,
            request=request,
            algorithms=algorithms_total,
        )

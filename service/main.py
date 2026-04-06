import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from service.adapters.inbound.consumer.rmq import AioPikaRMQConsumer, AioPikaRMQConsumerConnection, RMQQueueConsumer
from service.adapters.inbound.rest_api.fast_api_server import FastAPIServer
from service.adapters.inbound.rest_api.html_auth_middleware import AuthMiddleware
from service.adapters.outbound.producer.rmq import AioPikaRMQProducerConnection, AioPikaRMQProducer, \
    AioPikaRMQQueueBoundToExchangeCreator
from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.app_user import SAAppUserRepo
from service.adapters.outbound.repo.sa.impls.monitoring_algorithm import SAMonitoringAlgorithmRepo, \
    SAPeriodicMonitoringAlgorithmRepo, SASingleMonitoringAlgorithmRepo
from service.adapters.outbound.repo.sa.impls.payload import SAPayloadRepo
from service.adapters.outbound.repo.sa.impls.project import SAProjectRepo
from service.adapters.outbound.repo.sa.impls.refresh_token import SARefreshTokenRepo
from service.adapters.outbound.repo.sa.impls.task import SATaskRepo
from service.adapters.outbound.repo.sa.impls.task_group import SATaskGroupRepo
from service.adapters.outbound.repo.sa.impls.task_group_by_project import SATaskGroupByProjectRepo
from service.adapters.outbound.repo.sa.impls.task_run import SATaskRunRepo, SAWaitingTaskRunProvider, \
    SATaskRunMetricsProvider
from service.adapters.outbound.repo.sa.impls.task_run_status_log import SATaskRunStatusLogRepo
from service.adapters.outbound.repo.sa.impls.task_run_time_interval_execution_bounds import \
    SATaskRunTimeIntervalExecutionBoundsRepo
from service.adapters.outbound.repo.sa.impls.task_run_time_interval_progress import SATaskRunTimeIntervalProgressRepo
from service.adapters.outbound.repo.sa.impls.task_status_log import SATaskStatusLogRepo
from service.adapters.outbound.repo.sa.impls.time_interval_task_progress import SATimeIntervalTaskProgressRepo
from service.adapters.outbound.repo.sa.transaction import SATransactionFactory
from service.di import set_use_case_facade
from service.domain.services.balancing_algorithm.aimd import AIMDBalancingAlgorithm
from service.domain.services.balancing_algorithm.constant import ConstantBalancingAlgorithm
from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider
from service.domain.services.hasher import Hasher
from service.domain.services.log_cleaner import TaskRunStatusLogCleaner
from service.domain.services.payload_provider import PayloadProvider
from service.domain.services.task_progress_provider import ActualTimeIntervalExecutionBoundsProvider
from service.domain.services.token_service import TokenService
from service.domain.services.uniqueness_payload_checker import UniquenessPayloadChecker
from service.domain.use_cases.external.admin.activate_user import ActivateUserUC
from service.domain.use_cases.external.admin.deactivate_user import DeactivateUserUC
from service.domain.use_cases.external.admin.facade import AdminUseCaseFacade
from service.domain.use_cases.external.admin.get_all_users import GetAllUsersUC
from service.domain.use_cases.external.auth.create_first_admin import CreateFirstAdminUC, CreateFirstAdminUCRq
from service.domain.use_cases.external.auth.create_user import CreateUserUC
from service.domain.use_cases.external.auth.facade import AuthUseCaseFacade
from service.domain.use_cases.external.auth.get_me import GetMeUC
from service.domain.use_cases.external.auth.login import LoginUC
from service.domain.use_cases.external.auth.logout import LogoutUC
from service.domain.use_cases.external.auth.refresh_token import RefreshTokenUC
from service.domain.use_cases.external.auth.reset_password import ResetPasswordUC
from service.domain.use_cases.external.create_tasks import CreateTasksUC
from service.domain.use_cases.external.facade import UseCaseFacade
from service.domain.use_cases.external.get_payload import GetPayloadUC
from service.domain.use_cases.external.get_payloads import GetPayloadsUC
from service.domain.use_cases.external.get_task import GetTaskUC
from service.domain.use_cases.external.get_task_progress import GetTaskProgressUC
from service.domain.use_cases.external.get_task_runs import GetTaskRunsUC
from service.domain.use_cases.external.get_tasks import GetTasksUC
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUC
from service.domain.use_cases.external.monitoring_algorithm import CreateMonitoringAlgorithmUC, \
    GetAllMonitoringAlgorithmsUC, GetMonitoringAlgorithmUC
from service.domain.use_cases.external.update_payload import UpdatePayloadUC
from service.domain.use_cases.internal.create_task_runs import CreateTaskRunsUC, CreateTaskRunsUCRq
from service.domain.use_cases.internal.receive_task_run_execution_status import ReceiveTaskRunExecutionStatusUC, \
    ReceiveTaskRunExecutionStatusUCRq
from service.domain.use_cases.internal.retrieve_and_send_task_runs import RetrieveAndSendTaskRunsUC, \
    RetrieveAndSendTaskRunsUCRq
from service.domain.use_cases.internal.retrieve_waiting_task_runs import RetrieveWaitingTaskRunsUC
from service.domain.use_cases.internal.send_task_runs_to_execution import SendTaskRunsToExecutionUC
from service.domain.use_cases.internal.transit_task_run_status.abstract import TransitTaskRunStatusUCRq
from service.domain.use_cases.internal.transit_task_run_status.impls import TransitStatusFromExecutionToInterruptedUC, \
    TransitStatusFromQueuedToInterruptedUC, TransitStatusFromInterruptedToWaitingUC, \
    TransitStatusFromTempErrorToWaitingUC
from service.domain.use_cases.internal.transit_task_status import TransitTaskStatusUC, TransitTaskStatusUCRq
from service.ports.common.input_converter import InputConverterI
from service.ports.common.logs import logger, set_log_level
from service.ports.common.periodic_runner import PeriodicRunner
from service.ports.outbound.producer import DirectDataProducer
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.settings import ServiceSettings, ServiceType


class CommandResponseToReceiveTaskRunExecutionStatusUCRq(InputConverterI):

    def convert(self, raw_message: Any) -> Any:
        command_response = json.loads(raw_message)
        return ReceiveTaskRunExecutionStatusUCRq(command_response=command_response)


async def main():
    set_log_level(logging.INFO)
    settings = ServiceSettings()
    database = Database(settings.database_uri)

    transaction_factory = SATransactionFactory(database)
    monitoring_algorithm_repo = SAMonitoringAlgorithmRepo(database, models.MonitoringAlgorithm)
    periodic_monitoring_algorithm_repo = SAPeriodicMonitoringAlgorithmRepo(database, models.PeriodicMonitoringAlgorithm)
    single_monitoring_algorithm_repo = SASingleMonitoringAlgorithmRepo(database, models.SingleMonitoringAlgorithm)
    task_repo = SATaskRepo(database, models.Task)
    task_run_repo = SATaskRunRepo(database, models.TaskRun, chunk_size=2000)
    monitoring_algorithms = [periodic_monitoring_algorithm_repo, single_monitoring_algorithm_repo]
    task_to_execute_provider_registry = TaskToExecuteProviderRegistry(monitoring_algorithms)
    task_status_log_repo = SATaskStatusLogRepo(database, models.TaskStatusLog)
    task_run_status_log_repo = SATaskRunStatusLogRepo(database, models.TaskRunStatusLog)

    time_interval_task_progress_repo = SATimeIntervalTaskProgressRepo(database, models.TimeIntervalTaskProgress)
    task_run_time_interval_execution_bounds_repo = SATaskRunTimeIntervalExecutionBoundsRepo(database,
                                                                                            models.TaskRunTimeIntervalExecutionBounds)
    task_run_time_interval_progress_repo = SATaskRunTimeIntervalProgressRepo(database,
                                                                             models.TaskRunTimeIntervalProgress)
    waiting_task_run_provider = SAWaitingTaskRunProvider(database, task_run_repo)
    payload_repo = SAPayloadRepo(database, models.Payload)
    app_user_repo = SAAppUserRepo(database, models.AppUser)
    refresh_token_repo = SARefreshTokenRepo(database, models.RefreshToken)

    task_group_repo = SATaskGroupRepo(database, models.TaskGroup)
    project_repo = SAProjectRepo(database, models.TaskGroup)
    task_group_by_project_repo = SATaskGroupByProjectRepo(database, models.TaskGroupByProject)
    task_run_metrics_provider = SATaskRunMetricsProvider(database)

    rmq_producer_connection = AioPikaRMQProducerConnection.from_settings(settings.rmq_producer_connection)
    rmq_producer = AioPikaRMQProducer.from_settings(settings.rmq_producer_task_run, rmq_producer_connection)
    task_runs_producer = DirectDataProducer(settings.rmq_producer_task_run.routing_key, rmq_producer)
    queue_creator = AioPikaRMQQueueBoundToExchangeCreator(rmq_producer, rmq_producer_connection)

    execution_bounds_provider = DefaultExecutionBoundsProvider(
        task_run_time_interval_execution_bounds_repo=task_run_time_interval_execution_bounds_repo,
        default_left_date=datetime(2020, 1, 1),
        default_first_interval_days=31,
    )
    payload_provider = PayloadProvider(payload_repo)
    uniqueness_payload_checker = UniquenessPayloadChecker(payload_repo)
    actual_execution_bounds_provider = ActualTimeIntervalExecutionBoundsProvider(time_interval_task_progress_repo)
    task_status_log_cleaner = TaskRunStatusLogCleaner(task_run_status_log_repo)
    hasher = Hasher()
    token_service = TokenService(settings.jwt_secret_key)
    constant_balancing_algorithm = ConstantBalancingAlgorithm(500, task_group_repo,)
    aimd_balancing_algorithm = AIMDBalancingAlgorithm(task_group_repo,
                                                      task_run_metrics_provider, 10, 500,
                                                      50, 0.5,
                                                      600)
    # USE CASE
    create_monitoring_algorithm_uc = CreateMonitoringAlgorithmUC(monitoring_algorithm_repo,
                                                                 periodic_monitoring_algorithm_repo,
                                                                 single_monitoring_algorithm_repo,
                                                                 transaction_factory)
    get_all_monitoring_algorithms_uc = GetAllMonitoringAlgorithmsUC(monitoring_algorithms)
    create_tasks_uc = CreateTasksUC(transaction_factory, uniqueness_payload_checker, payload_repo, task_repo,
                                    task_status_log_repo)
    get_tasks_uc = GetTasksUC(task_repo)
    get_task_uc = GetTaskUC(task_repo)
    get_task_runs_uc = GetTaskRunsUC(task_repo, task_run_repo)
    get_task_progress_uc = GetTaskProgressUC(task_repo, time_interval_task_progress_repo)
    get_payload_uc = GetPayloadUC(payload_repo)
    get_payloads_uc = GetPayloadsUC(payload_repo)
    update_payload_uc = UpdatePayloadUC(payload_repo)
    get_monitoring_algorithm_uc = GetMonitoringAlgorithmUC(monitoring_algorithm_repo,
                                                           periodic_monitoring_algorithm_repo,
                                                           single_monitoring_algorithm_repo, transaction_factory)
    get_tasks_detailed_uc = GetTasksDetailedUC(get_tasks_uc, get_payload_uc, get_monitoring_algorithm_uc, task_repo)
    use_case_facade = UseCaseFacade(create_tasks_uc, create_monitoring_algorithm_uc, get_all_monitoring_algorithms_uc,
                                    get_tasks_uc, get_task_uc, get_task_runs_uc, get_task_progress_uc, get_payloads_uc,
                                    get_payload_uc, update_payload_uc, get_tasks_detailed_uc)
    set_use_case_facade(use_case_facade)

    create_task_runs_uc = CreateTaskRunsUC(task_repo, task_run_repo, task_status_log_repo, task_run_status_log_repo,
                                           task_run_time_interval_execution_bounds_repo,
                                           transaction_factory, task_to_execute_provider_registry,
                                           execution_bounds_provider,
                                           payload_provider, actual_execution_bounds_provider,
                                           task_group_repo)
    receive_task_run_execution_status_uc = ReceiveTaskRunExecutionStatusUC(task_run_repo,
                                                                           task_run_status_log_repo,
                                                                           time_interval_task_progress_repo,
                                                                           transaction_factory)
    retrieve_waiting_task_runs_uc = RetrieveWaitingTaskRunsUC(task_run_repo,
                                                              task_run_status_log_repo,
                                                              transaction_factory,
                                                              waiting_task_run_provider,
                                                              aimd_balancing_algorithm,)
    send_task_runs_to_execution_uc = SendTaskRunsToExecutionUC(task_runs_producer, queue_creator)
    retrieve_and_send_task_runs_uc = RetrieveAndSendTaskRunsUC(retrieve_waiting_task_runs_uc,
                                                               send_task_runs_to_execution_uc)

    transit_status_from_queued_to_interrupted_uc = TransitStatusFromQueuedToInterruptedUC(task_run_repo,
                                                                                          task_run_status_log_repo,
                                                                                          transaction_factory)
    transit_status_from_execution_to_interrupted_uc = TransitStatusFromExecutionToInterruptedUC(task_run_repo,
                                                                                                task_run_status_log_repo,
                                                                                                transaction_factory)
    transit_status_from_interrupted_to_waiting_uc = TransitStatusFromInterruptedToWaitingUC(task_run_repo,
                                                                                            task_run_status_log_repo,
                                                                                            transaction_factory)
    transit_status_from_temp_error_to_waiting_uc = TransitStatusFromTempErrorToWaitingUC(task_run_repo,
                                                                                         task_run_status_log_repo,
                                                                                         transaction_factory)

    transit_task_status_uc = TransitTaskStatusUC(
        task_repo=task_repo,
        task_run_repo=task_run_repo,
        task_status_log_repo=task_status_log_repo,
        transaction_factory=transaction_factory,
    )

    # USE CASE: Auth
    create_user_uc = CreateUserUC(app_user_repo, hasher)
    create_first_admin_uc = CreateFirstAdminUC(create_user_uc, app_user_repo)
    login_uc = LoginUC(app_user_repo, refresh_token_repo, hasher, token_service)
    logout_uc = LogoutUC(refresh_token_repo)
    refresh_token_uc = RefreshTokenUC(app_user_repo, refresh_token_repo, token_service)
    get_me_uc = GetMeUC(app_user_repo)
    reset_password_uc = ResetPasswordUC(app_user_repo, hasher)
    auth_use_case_facade = AuthUseCaseFacade(login_uc, logout_uc, refresh_token_uc, get_me_uc,create_user_uc,
                                             reset_password_uc)

    # USE CASE: Admin
    deactivate_user_uc = DeactivateUserUC(app_user_repo)
    get_all_users_uc = GetAllUsersUC(app_user_repo)
    activate_user_uc = ActivateUserUC(app_user_repo)
    admin_use_case_facade = AdminUseCaseFacade(deactivate_user_uc, get_all_users_uc, activate_user_uc)

    rmq_consumer_connection = AioPikaRMQConsumerConnection.from_settings(settings.rmq_consumer_connection)
    rmq_consumer = AioPikaRMQConsumer.from_settings(settings.rmq_consumer, rmq_consumer_connection)
    rmq_task_run_execution_status_consumer = RMQQueueConsumer(rmq_consumer,
                                                              settings.rmq_task_run_execution_status_queue,
                                                              receive_task_run_execution_status_uc.apply,
                                                              CommandResponseToReceiveTaskRunExecutionStatusUCRq())

    fastapi_server = FastAPIServer.from_settings(settings.fastapi_server)
    fastapi_server.app.state.auth_facade = auth_use_case_facade
    fastapi_server.app.state.use_case_facade = use_case_facade
    fastapi_server.app.state.token_service = token_service
    fastapi_server.app.state.admin_use_case_facade = admin_use_case_facade
    fastapi_server.app.add_middleware(AuthMiddleware)

    startable = [
        rmq_producer_connection,
        rmq_producer,
        rmq_consumer_connection,
        rmq_consumer,
        rmq_task_run_execution_status_consumer,

    ]
    periodic_runners = [
        PeriodicRunner(create_task_runs_uc.apply, 30, run_name="Create task runs from tasks",
                       method_args=[CreateTaskRunsUCRq()]),
        PeriodicRunner(transit_status_from_queued_to_interrupted_uc.apply, 30, run_name="QUEUED -> INTERRUPTED",
                       method_args=[TransitTaskRunStatusUCRq(ttl_seconds=300)]),
        PeriodicRunner(transit_status_from_execution_to_interrupted_uc.apply, 30, run_name="EXECUTION -> INTERRUPTED",
                       method_args=[TransitTaskRunStatusUCRq(ttl_seconds=300)]),
        PeriodicRunner(transit_status_from_interrupted_to_waiting_uc.apply, 30, run_name="INTERRUPTED -> WAITING",
                       method_args=[TransitTaskRunStatusUCRq(ttl_seconds=0)]),
        PeriodicRunner(transit_status_from_temp_error_to_waiting_uc.apply, 30, run_name="TEMP_ERROR -> WAITING",
                       method_args=[TransitTaskRunStatusUCRq(ttl_seconds=30)]),
        PeriodicRunner(retrieve_and_send_task_runs_uc.apply, 30, run_name="Send task runs to execution",
                       method_args=[RetrieveAndSendTaskRunsUCRq()]),
        PeriodicRunner(transit_task_status_uc.apply, 30, run_name="Transit task status to SUCCEED or ERROR",
                       method_args=[TransitTaskStatusUCRq()]),
        PeriodicRunner(task_status_log_cleaner.clean_logs, 86_400, 30, run_name="Clean task run status logs"),
        PeriodicRunner(receive_task_run_execution_status_uc.upload_command_responses, 30,
                       run_name="Upload received task run statuses"),

    ]
    logger.info(f"service configured as {settings.service_type}")
    if settings.service_type in (ServiceType.WORKER, ServiceType.MONOLITH):
        for startable_obj in startable:
            await startable_obj.start()
        for periodic_runner in periodic_runners:
            periodic_runner.create_periodic_task()
    try:
        if settings.service_type in (ServiceType.API, ServiceType.MONOLITH):
            create_first_admin_uc_rs = await create_first_admin_uc.apply(
                CreateFirstAdminUCRq(username=settings.admin_username,
                                     password=settings.admin_password))
            logger.info(
                f"First admin created: {create_first_admin_uc_rs.success}; detail: {create_first_admin_uc_rs.error}")
            await fastapi_server.start()
        await asyncio.Future()
    except BaseException as e:
        logger.critical(f"Stop service due to error: {e.__class__.__name__}: {e}")
    finally:
        if settings.service_type in (ServiceType.WORKER, ServiceType.MONOLITH):
            for periodic_runner in periodic_runners:
                periodic_runner.cancel()
            for startable_obj in startable:
                await startable_obj.stop()
        if settings.service_type in (ServiceType.API, ServiceType.MONOLITH):
            await fastapi_server.stop()


if __name__ == '__main__':
    asyncio.run(main())

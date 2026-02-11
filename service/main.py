import asyncio
import logging
from datetime import datetime

from service.adapters.inbound.consumer.rmq import AioPikaRMQConsumer, AioPikaRMQConsumerConnection, RMQQueueConsumer
from service.adapters.outbound.producer.rmq import AioPikaRMQProducerConnection, AioPikaRMQProducer
from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.base import Base
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.monitoring_algorithm import SAMonitoringAlgorithmRepo, \
    SAPeriodicMonitoringAlgorithmRepo
from service.adapters.outbound.repo.sa.impls.payload import SAPayloadRepo
from service.adapters.outbound.repo.sa.impls.task import SATaskRepo
from service.adapters.outbound.repo.sa.impls.task_run import SATaskRunRepo
from service.adapters.outbound.repo.sa.impls.task_run_status_log import SATaskRunStatusLogRepo
from service.adapters.outbound.repo.sa.impls.task_status_log import SATaskStatusLogRepo
from service.adapters.outbound.repo.sa.impls.time_interval_task_progress import SATimeIntervalTaskProgressRepo
from service.adapters.outbound.repo.sa.transaction import SATransactionFactory
from service.domain.services.execution_bounds_provider import DefaultExecutionBoundsProvider
from service.domain.services.payload_provider import PayloadProvider
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
from service.ports.common.input_converter import FromStrOrBytesToPydantic
from service.ports.common.logs import logger, set_log_level
from service.ports.common.periodic_runner import PeriodicRunner
from service.ports.outbound.producer import DirectDataProducer
from service.ports.outbound.repo.monitoring_algorithm import TaskToExecuteProviderRegistry
from service.settings import ServiceSettings


async def main():
    set_log_level(logging.INFO)
    settings = ServiceSettings()
    database = Database(settings.database_uri)

    # Создаём таблицы
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    transaction_factory = SATransactionFactory(database)
    monitoring_algorithm_repo = SAMonitoringAlgorithmRepo(database, models.MonitoringAlgorithm)
    periodic_monitoring_algorithm_repo = SAPeriodicMonitoringAlgorithmRepo(database, models.PeriodicMonitoringAlgorithm)
    task_repo = SATaskRepo(database, models.Task)
    task_run_repo = SATaskRunRepo(database, models.TaskRun)
    task_to_execute_provider_registry = TaskToExecuteProviderRegistry([periodic_monitoring_algorithm_repo])
    task_status_log_repo = SATaskStatusLogRepo(database, models.TaskStatusLog)
    task_run_status_log_repo = SATaskRunStatusLogRepo(database, models.TaskRunStatusLog)

    time_interval_task_progress_repo = SATimeIntervalTaskProgressRepo(database, models.TimeIntervalTaskProgress)
    payload_repo = SAPayloadRepo(database, models.Payload)

    rmq_producer_connection = AioPikaRMQProducerConnection.from_settings(settings.rmq_producer_connection)
    rmq_producer = AioPikaRMQProducer.from_settings(settings.rmq_producer_task_run, rmq_producer_connection)
    task_runs_producer = DirectDataProducer(settings.rmq_producer_task_run.routing_key, rmq_producer)

    execution_bounds_provider = DefaultExecutionBoundsProvider(
        time_interval_progress_repo=time_interval_task_progress_repo,
        default_left_date=datetime(2010, 1, 1),
        default_first_interval_days=31,
    )
    payload_provider = PayloadProvider(payload_repo)

    # USE CASE
    create_task_runs_uc = CreateTaskRunsUC(task_repo, task_run_repo, task_status_log_repo, task_run_status_log_repo,
                                           transaction_factory, task_to_execute_provider_registry,
                                           execution_bounds_provider,
                                           payload_provider)
    receive_task_run_execution_status_uc = ReceiveTaskRunExecutionStatusUC(task_run_repo,
                                                                           task_run_status_log_repo,
                                                                           time_interval_task_progress_repo,
                                                                           transaction_factory)
    retrieve_waiting_task_runs_uc = RetrieveWaitingTaskRunsUC(task_run_repo,
                                                              task_run_status_log_repo,
                                                              transaction_factory)
    send_task_runs_to_execution_uc = SendTaskRunsToExecutionUC(task_runs_producer)
    retrieve_and_send_task_runs_uc = RetrieveAndSendTaskRunsUC(retrieve_waiting_task_runs_uc, send_task_runs_to_execution_uc)

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

    rmq_consumer_connection = AioPikaRMQConsumerConnection.from_settings(settings.rmq_consumer_connection)
    rmq_consumer = AioPikaRMQConsumer.from_settings(settings.rmq_consumer, rmq_consumer_connection)
    rmq_task_run_execution_status_consumer = RMQQueueConsumer(rmq_consumer,
                                                              settings.rmq_task_run_execution_status_queue,
                                                              receive_task_run_execution_status_uc.apply,
                                                              FromStrOrBytesToPydantic(
                                                                  ReceiveTaskRunExecutionStatusUCRq))

    startable = [rmq_producer_connection, rmq_producer, rmq_consumer_connection, rmq_consumer,
                 rmq_task_run_execution_status_consumer, ]
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

    ]
    for startable_obj in startable:
        await startable_obj.start()
    for periodic_runner in periodic_runners:
        periodic_runner.create_periodic_task()
    try:
        await asyncio.Future()
    except BaseException as e:
        logger.critical(f"Stop service due to error: {e.__class__.__name__}: {e}")
    finally:
        for periodic_runner in periodic_runners:
            periodic_runner.cancel()
        for startable_obj in startable:
            await startable_obj.stop()


if __name__ == '__main__':
    asyncio.run(main())

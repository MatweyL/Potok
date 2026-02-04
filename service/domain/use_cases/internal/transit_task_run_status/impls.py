from functools import cached_property

from service.domain.use_cases.internal.transit_task_run_status.abstract import AbstractTransitTaskRunStatusUC

from service.domain.schemas.enums import TaskRunStatus


class TransitStatusFromQueuedToInterruptedUC(AbstractTransitTaskRunStatusUC):
    """
    Переводит TaskRun из статуса QUEUED в INTERRUPTED, если задача пробыла
    в статусе QUEUED дольше, чем ttl_seconds.
    """

    @cached_property
    def from_status(self) -> TaskRunStatus:
        return TaskRunStatus.QUEUED

    @cached_property
    def to_status(self) -> TaskRunStatus:
        return TaskRunStatus.INTERRUPTED


class TransitStatusFromExecutionToInterruptedUC(AbstractTransitTaskRunStatusUC):
    """
    Переводит TaskRun из статуса EXECUTION в INTERRUPTED, если задача пробыла
    в статусе EXECUTION дольше, чем ttl_seconds.
    """

    @cached_property
    def from_status(self) -> TaskRunStatus:
        return TaskRunStatus.EXECUTION

    @cached_property
    def to_status(self) -> TaskRunStatus:
        return TaskRunStatus.INTERRUPTED


class TransitStatusFromInterruptedToWaitingUC(AbstractTransitTaskRunStatusUC):
    """
    Переводит TaskRun из статуса INTERRUPTED в WAITING.
    """

    @cached_property
    def from_status(self) -> TaskRunStatus:
        return TaskRunStatus.INTERRUPTED

    @cached_property
    def to_status(self) -> TaskRunStatus:
        return TaskRunStatus.WAITING


class TransitStatusFromTempErrorToWaitingUC(AbstractTransitTaskRunStatusUC):
    """
    Переводит TaskRun из статуса INTERRUPTED в WAITING.
    """

    @cached_property
    def from_status(self) -> TaskRunStatus:
        return TaskRunStatus.TEMP_ERROR

    @cached_property
    def to_status(self) -> TaskRunStatus:
        return TaskRunStatus.WAITING

from datetime import datetime
from tkinter.tix import INTEGER
from typing import Dict, List

from sqlalchemy import JSON, BIGINT, ForeignKey, VARCHAR, Enum, INT, DateTime, FLOAT
from sqlalchemy.orm import Mapped, mapped_column

from service.adapters.outbound.repo.sa.base import Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin, \
    SerialIntPKMixin
from service.domain.schemas.enums import TaskStatus, TaskType, PriorityType, MonitoringAlgorithmType


class Payload(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    data: Mapped[dict] = mapped_column(JSON, )


class Task(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    group_name: Mapped[str] = mapped_column(VARCHAR(64))
    priority: Mapped[PriorityType] = mapped_column(Enum(PriorityType))
    type: Mapped[TaskType] = mapped_column(Enum(TaskType))
    monitoring_algorithm_id: Mapped[int] = mapped_column(INT, ForeignKey("monitoring_algorithm.id"))
    execution_arguments: Mapped[Dict] = mapped_column(JSON, nullable=True)

    status: Mapped[TaskStatus] = mapped_column(Enum(TaskStatus))
    status_updated_at: Mapped[datetime] = mapped_column(DateTime)
    payload_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("payload.id"))


class MonitoringAlgorithm(Base, TablenameMixin, SerialIntPKMixin, LoadTimestampMixin):
    type: Mapped[MonitoringAlgorithmType] = mapped_column(Enum(MonitoringAlgorithmType))


class PeriodicMonitoringAlgorithm(Base, TablenameMixin, LoadTimestampMixin):
    id: Mapped[int] = mapped_column(INT, ForeignKey("monitoring_algorithm.id"), primary_key=True)
    timeout: Mapped[float] = mapped_column(FLOAT)
    timeout_noize: Mapped[float] = mapped_column(FLOAT)


class SingleMonitoringAlgorithm(Base, TablenameMixin, LoadTimestampMixin):
    id: Mapped[int] = mapped_column(INT, ForeignKey("monitoring_algorithm.id"), primary_key=True)
    timeouts: Mapped[List[float]] = mapped_column(JSON)
    timeout_noize: Mapped[float] = mapped_column(FLOAT)

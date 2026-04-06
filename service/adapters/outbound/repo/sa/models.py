from datetime import datetime
from typing import Dict, List

from sqlalchemy import JSON, BIGINT, ForeignKey, VARCHAR, Enum, INT, DateTime, FLOAT, TEXT, UUID, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from service.adapters.outbound.repo.sa.base import Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin, \
    SerialIntPKMixin, JSONWithDatetime
from service.domain.schemas.enums import TaskStatus, TaskType, PriorityType, MonitoringAlgorithmType, TaskRunStatus, \
    AppUserRole


class Payload(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    data: Mapped[dict] = mapped_column(JSONWithDatetime, )
    checksum: Mapped[str] = mapped_column(UUID(as_uuid=True), nullable=False)


class Task(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    group_id: Mapped[int] = mapped_column(INT, ForeignKey("task_group.id"))
    priority: Mapped[PriorityType] = mapped_column(Enum(PriorityType))
    type: Mapped[TaskType] = mapped_column(Enum(TaskType))
    monitoring_algorithm_id: Mapped[int] = mapped_column(INT, ForeignKey("monitoring_algorithm.id"))
    execution_arguments: Mapped[Dict] = mapped_column(JSONWithDatetime, nullable=True)

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


class TaskRun(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    task_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task.id"), )
    group_name: Mapped[str] = mapped_column(VARCHAR(64))
    priority: Mapped[PriorityType] = mapped_column(Enum(PriorityType))
    type: Mapped[TaskType] = mapped_column(Enum(TaskType))
    execution_arguments: Mapped[Dict] = mapped_column(JSONWithDatetime, nullable=True)
    execution_bounds: Mapped[Dict] = mapped_column(JSONWithDatetime, nullable=True)
    payload: Mapped[Dict] = mapped_column(JSONWithDatetime, nullable=True)

    status: Mapped[TaskRunStatus] = mapped_column(Enum(TaskRunStatus))
    status_updated_at: Mapped[datetime] = mapped_column(DateTime)
    description: Mapped[str] = mapped_column(TEXT, nullable=True)


class TaskRunStatusLog(Base, TablenameMixin, LoadTimestampMixin):
    task_run_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task_run.id"), primary_key=True)
    status_updated_at: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    status: Mapped[TaskRunStatus] = mapped_column(Enum(TaskRunStatus))
    description: Mapped[str] = mapped_column(TEXT, nullable=True)


class TaskStatusLog(Base, TablenameMixin, LoadTimestampMixin):
    task_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task.id"), primary_key=True)
    status_updated_at: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    status: Mapped[TaskStatus] = mapped_column(Enum(TaskStatus))
    description: Mapped[str] = mapped_column(TEXT, nullable=True)


class TimeIntervalTaskProgress(Base, TablenameMixin, LoadTimestampMixin):
    task_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task.id"), primary_key=True)
    right_bound_at: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    left_bound_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    collected_data_amount: Mapped[int] = mapped_column(INT, )
    saved_data_amount: Mapped[int] = mapped_column(INT, )


class TaskRunTimeIntervalExecutionBounds(Base, TablenameMixin, LoadTimestampMixin):
    task_run_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task_run.id"), primary_key=True)
    task_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task.id"), )
    right_bound_at: Mapped[datetime] = mapped_column(DateTime, )
    left_bound_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)


class AppUser(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    roles: Mapped[List[AppUserRole]] = mapped_column(JSON, nullable=False)
    username: Mapped[str] = mapped_column(TEXT, nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(TEXT, nullable=False, )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)


class RefreshToken(Base, TablenameMixin, SerialBigIntPKMixin, LoadTimestampMixin):
    user_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("app_user.id"), primary_key=True)
    token: Mapped[str] = mapped_column(TEXT, nullable=False, )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Project(Base, TablenameMixin, SerialIntPKMixin, LoadTimestampMixin):
    title: Mapped[str] = mapped_column(TEXT, nullable=False, )
    description: Mapped[str] = mapped_column(TEXT, nullable=False, )


class TaskGroup(Base, TablenameMixin, SerialIntPKMixin, LoadTimestampMixin):
    title: Mapped[str] = mapped_column(TEXT, nullable=False, )
    description: Mapped[str] = mapped_column(TEXT, nullable=False, )
    name: Mapped[str] = mapped_column(TEXT, nullable=False, )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    queue_per_priority: Mapped[bool] = mapped_column(Boolean, nullable=False)


class TaskGroupByProject(Base, TablenameMixin, LoadTimestampMixin):
    group_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task_group.id"), primary_key=True)
    project_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("project.id"), primary_key=True)


class TaskRunTimeIntervalProgress(Base, TablenameMixin, LoadTimestampMixin):
    task_run_id: Mapped[int] = mapped_column(BIGINT, ForeignKey("task_run.id"), primary_key=True)
    right_bound_at: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    left_bound_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    collected_data_amount: Mapped[int] = mapped_column(INT, )
    saved_data_amount: Mapped[int] = mapped_column(INT, )

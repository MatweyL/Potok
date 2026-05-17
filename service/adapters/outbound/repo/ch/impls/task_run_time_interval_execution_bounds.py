from typing import Any, Dict

from clickhouse_connect.driver.asyncclient import AsyncClient

from service.adapters.outbound.repo.ch.abstract import AbstractCHRepo, ColumnDef, TableSchema
from service.domain.schemas.execution_bounds import TimeIntervalBounds
from service.domain.schemas.task_run import TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK


class CHTaskRunTimeIntervalExecutionBoundsRepo(
    AbstractCHRepo[TaskRunTimeIntervalExecutionBounds, TaskRunTimeIntervalExecutionBoundsPK]
):

    @property
    def table_name(self) -> str:
        return "task_run_time_interval_execution_bounds"

    @property
    def table_schema(self) -> TableSchema:
        return TableSchema(
            columns=[
                ColumnDef("task_run_id",   "Int64",             comment="FK → task_run.id; первичный ключ"),
                ColumnDef("task_id",       "Int64",             comment="FK → task.id"),
                ColumnDef("right_bound_at", "DateTime",         comment="Правая граница исполнения"),
                ColumnDef("left_bound_at",  "Nullable(DateTime)", comment="Левая граница исполнения"),
                ColumnDef("loaded_at",      "Nullable(DateTime)",         default="now()", comment="Время вставки строки"),
            ],
            # PK у этой таблицы — только task_run_id (один run = одни bounds)
            order_by=["task_run_id"],
            partition_by="toYYYYMM(right_bound_at)",
            engine="MergeTree()",
        )

    def to_domain(self, row: Dict[str, Any]) -> TaskRunTimeIntervalExecutionBounds:
        return TaskRunTimeIntervalExecutionBounds(
            task_run_id=row["task_run_id"],
            task_id=row["task_id"],
            execution_bounds=TimeIntervalBounds(
                right_bound_at=row["right_bound_at"],
                left_bound_at=row.get("left_bound_at"),
            ),
        )

    def to_row(self, obj: TaskRunTimeIntervalExecutionBounds) -> Dict[str, Any]:
        return {
            "task_run_id":   obj.task_run_id,
            "task_id":       obj.task_id,
            "right_bound_at": obj.execution_bounds.right_bound_at,
            "left_bound_at":  obj.execution_bounds.left_bound_at,
        }

    def pk_to_where(self, pk: TaskRunTimeIntervalExecutionBoundsPK) -> tuple[str, Dict[str, Any]]:
        return (
            "task_run_id = {task_run_id:Int64}",
            {"task_run_id": pk.task_run_id},
        )


async def create_task_run_time_interval_execution_bounds_repo(
    client: AsyncClient,
) -> CHTaskRunTimeIntervalExecutionBoundsRepo:
    repo = CHTaskRunTimeIntervalExecutionBoundsRepo(client=client)
    await repo.create_table_if_not_exists()
    return repo
from typing import Any, Dict

from clickhouse_connect.driver.asyncclient import AsyncClient

from service.adapters.outbound.repo.ch.abstract import AbstractCHRepo, ColumnDef, TableSchema
from service.domain.schemas.task_run import TaskRunTimeIntervalProgress, TaskRunTimeIntervalProgressPK


class CHTaskRunTimeIntervalProgressRepo(AbstractCHRepo[TaskRunTimeIntervalProgress, TaskRunTimeIntervalProgressPK]):

    @property
    def table_name(self) -> str:
        return "task_run_time_interval_progress"

    @property
    def table_schema(self) -> TableSchema:
        return TableSchema(
            columns=[
                ColumnDef("task_run_id",           "Int64",            comment="FK → task_run.id"),
                ColumnDef("right_bound_at",         "DateTime",         comment="Правая граница временного интервала"),
                ColumnDef("left_bound_at",          "Nullable(DateTime)", comment="Левая граница временного интервала"),
                ColumnDef("collected_data_amount",  "Int32",            comment="Количество собранных записей"),
                ColumnDef("saved_data_amount",      "Int32",            comment="Количество сохранённых записей"),
                ColumnDef("loaded_at",              "Nullable(DateTime)",         default="now()", comment="Время вставки строки"),
            ],
            order_by=["task_run_id", "right_bound_at"],
            partition_by="toYYYYMM(right_bound_at)",
            engine="MergeTree()",
        )

    def to_domain(self, row: Dict[str, Any]) -> TaskRunTimeIntervalProgress:
        return TaskRunTimeIntervalProgress(
            task_run_id=row["task_run_id"],
            right_bound_at=row["right_bound_at"],
            left_bound_at=row.get("left_bound_at"),
            collected_data_amount=row["collected_data_amount"],
            saved_data_amount=row["saved_data_amount"],
        )

    def to_row(self, obj: TaskRunTimeIntervalProgress) -> Dict[str, Any]:
        return {
            "task_run_id":          obj.task_run_id,
            "right_bound_at":       obj.right_bound_at,
            "left_bound_at":        obj.left_bound_at,
            "collected_data_amount": obj.collected_data_amount,
            "saved_data_amount":    obj.saved_data_amount,
        }

    def pk_to_where(self, pk: TaskRunTimeIntervalProgressPK) -> tuple[str, Dict[str, Any]]:
        return (
            "task_run_id = {task_run_id:Int64}"
            " AND right_bound_at = {right_bound_at:DateTime}",
            {
                "task_run_id":    pk.task_run_id,
                "right_bound_at": pk.right_bound_at,
            },
        )


async def create_task_run_time_interval_progress_repo(
    client: AsyncClient,
) -> CHTaskRunTimeIntervalProgressRepo:
    repo = CHTaskRunTimeIntervalProgressRepo(client=client)
    await repo.create_table_if_not_exists()
    return repo
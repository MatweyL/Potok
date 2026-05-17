from typing import Any, Dict, List

from clickhouse_connect.driver.asyncclient import AsyncClient

from service.adapters.outbound.repo.ch.abstract import AbstractCHRepo, ColumnDef, TableSchema
from service.domain.schemas.task_progress import TimeIntervalTaskProgress, TimeIntervalTaskProgressPK


class CHTimeIntervalTaskProgressRepo(AbstractCHRepo[TimeIntervalTaskProgress, TimeIntervalTaskProgressPK]):

    @property
    def table_name(self) -> str:
        return "time_interval_task_progress"

    @property
    def table_schema(self) -> TableSchema:
        return TableSchema(
            columns=[
                ColumnDef("task_id",               "Int64",             comment="FK → task.id"),
                ColumnDef("right_bound_at",         "DateTime",         comment="Правая граница временного интервала"),
                ColumnDef("left_bound_at",          "Nullable(DateTime)", comment="Левая граница временного интервала"),
                ColumnDef("collected_data_amount",  "Int32",            comment="Количество собранных записей"),
                ColumnDef("saved_data_amount",      "Int32",            comment="Количество сохранённых записей"),
                ColumnDef("loaded_at",              "Nullable(DateTime)",         default="now()", comment="Время вставки строки"),
            ],
            order_by=["task_id", "right_bound_at"],
            partition_by="toYYYYMM(right_bound_at)",
            engine="MergeTree()",
        )

    def to_domain(self, row: Dict[str, Any]) -> TimeIntervalTaskProgress:
        return TimeIntervalTaskProgress(
            task_id=row["task_id"],
            right_bound_at=row["right_bound_at"],
            left_bound_at=row.get("left_bound_at"),
            collected_data_amount=row["collected_data_amount"],
            saved_data_amount=row["saved_data_amount"],
        )

    def to_row(self, obj: TimeIntervalTaskProgress) -> Dict[str, Any]:
        return {
            "task_id":              obj.task_id,
            "right_bound_at":       obj.right_bound_at,
            "left_bound_at":        obj.left_bound_at,
            "collected_data_amount": obj.collected_data_amount,
            "saved_data_amount":    obj.saved_data_amount,
        }

    def pk_to_where(self, pk: TimeIntervalTaskProgressPK) -> tuple[str, Dict[str, Any]]:
        return (
            "task_id = {task_id:Int64}"
            " AND right_bound_at = {right_bound_at:DateTime}",
            {
                "task_id":        pk.task_id,
                "right_bound_at": pk.right_bound_at,
            },
        )

    async def get_by_task_ids_ordered(self, task_ids: List[int]) -> List[TimeIntervalTaskProgress]:
        if not task_ids:
            return []
        query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE task_id IN {{task_ids:Array(Int64)}}
            ORDER BY task_id, right_bound_at DESC
        """
        result = await self._client.query(query, parameters={"task_ids": task_ids})
        return [self.to_domain(row) for row in result.named_results()]


async def create_time_interval_task_progress_repo(
    client: AsyncClient,
) -> CHTimeIntervalTaskProgressRepo:
    repo = CHTimeIntervalTaskProgressRepo(client=client)
    await repo.create_table_if_not_exists()
    return repo
"""
Пример конкретной реализации AbstractCHRepo для TaskRunStatusLog.

Демонстрирует:
  - реализацию всех обязательных методов,
  - описание схемы таблицы через TableSchema / ColumnDef,
  - задание TTL (90 дней) — вшивается в CREATE TABLE автоматически,
  - автоматическое создание таблицы при старте приложения.
"""

from typing import Any, Dict, Optional

from clickhouse_connect.driver.asyncclient import AsyncClient

from service.adapters.outbound.repo.ch.abstract import AbstractCHRepo, ColumnDef, TableSchema, TTLRule
from service.domain.schemas.task_run import TaskRunStatusLog, TaskRunStatusLogPK


class CHTaskRunStatusLogRepo(AbstractCHRepo[TaskRunStatusLog, TaskRunStatusLogPK]):

    # ------------------------------------------------------------------
    # Имя таблицы
    # ------------------------------------------------------------------

    @property
    def table_name(self) -> str:
        return "task_run_status_log"

    # ------------------------------------------------------------------
    # Схема таблицы — используется для CREATE TABLE и INSERT
    # ------------------------------------------------------------------

    @property
    def table_schema(self) -> TableSchema:
        return TableSchema(
            columns=[
                ColumnDef(
                    name="task_run_id",
                    ch_type="Int64",
                    comment="FK → task_run.id",
                ),
                ColumnDef(
                    name="status_updated_at",
                    ch_type="DateTime",
                    comment="Момент смены статуса; используется как TTL-колонка",
                ),
                ColumnDef(
                    name="status",
                    ch_type="LowCardinality(String)",
                    comment="Значение TaskRunStatus",
                ),
                ColumnDef(
                    name="description",
                    ch_type="Nullable(String)",
                    codec="CODEC(ZSTD(3))",
                    comment="Опциональное описание / причина смены статуса",
                ),
                # Служебная колонка — момент загрузки строки в CH
                ColumnDef(
                    name="loaded_at",
                    ch_type="Nullable(DateTime)",
                    default="now()",
                    comment="Время вставки строки (проставляется автоматически)",
                ),
            ],
            # Первичная сортировка: сначала по задаче, потом по времени
            order_by=["task_run_id", "status_updated_at"],
            # Партиционирование по месяцам — TTL сможет удалять целые партиции
            partition_by="toYYYYMM(status_updated_at)",
            engine="MergeTree()",
            settings={
                # Дедупликация при вставке одинаковых блоков (идемпотентность)
                "non_replicated_deduplication_window": 100,
            },
        )

    # ------------------------------------------------------------------
    # TTL: строки живут 90 дней с момента смены статуса
    # ------------------------------------------------------------------

    @property
    def ttl_rule(self) -> Optional[TTLRule]:
        return TTLRule(
            column="status_updated_at",
            interval_seconds=86400 * 90,   # 90 дней
            # action=None → ClickHouse удалит строки (DELETE по умолчанию)
            #
            # Для архивирования на холодный диск вместо удаления:
            # action="TO DISK 'cold'"
        )

    # ------------------------------------------------------------------
    # Конвертация домен ↔ строка CH
    # ------------------------------------------------------------------

    def to_domain(self, row: Dict[str, Any]) -> TaskRunStatusLog:
        return TaskRunStatusLog(
            task_run_id=row["task_run_id"],
            status_updated_at=row["status_updated_at"],
            status=row["status"],
            description=row.get("description"),
        )

    def to_row(self, obj: TaskRunStatusLog) -> Dict[str, Any]:
        return {
            "task_run_id": obj.task_run_id,
            "status_updated_at": obj.status_updated_at,
            "status": obj.status,
            "description": obj.description,
            # loaded_at не указываем — CH подставит DEFAULT now()
        }

    def pk_to_where(self, pk: TaskRunStatusLogPK) -> tuple[str, Dict[str, Any]]:
        return (
            "task_run_id = {task_run_id:Int64}"
            " AND status_updated_at = {status_updated_at:DateTime}",
            {
                "task_run_id": pk.task_run_id,
                "status_updated_at": pk.status_updated_at,
            },
        )


# ------------------------------------------------------------------
# Фабрика — вызывать при старте приложения
# ------------------------------------------------------------------

async def create_task_run_status_log_repo(
    client: AsyncClient,
) -> CHTaskRunStatusLogRepo:
    """
    Создаёт репозиторий и при необходимости инициализирует таблицу в CH.

    Таблица создаётся с TTL, вшитым в DDL — отдельный apply_ttl() не нужен.

    Пример:
        client = await clickhouse_connect.get_async_client(host="localhost")
        repo = await create_task_run_status_log_repo(client)
    """
    repo = CHTaskRunStatusLogRepo(client=client)
    await repo.create_table_if_not_exists()
    return repo


# ------------------------------------------------------------------
# Утилита: посмотреть DDL без подключения к CH
# ------------------------------------------------------------------

if __name__ == "__main__":
    dummy_repo = object.__new__(CHTaskRunStatusLogRepo)
    print(dummy_repo.get_create_table_ddl())
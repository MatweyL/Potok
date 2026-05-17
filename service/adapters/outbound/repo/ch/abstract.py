"""
Абстрактный репозиторий для ClickHouse.

Особенности ClickHouse, которые учтены здесь:
  - Нет транзакций в классическом понимании (используем session-like контекст через AsyncClient).
  - UPDATE/DELETE — мутации, асинхронные и дорогие; по возможности избегаем.
  - INSERT работает через batch (оптимально).
  - TTL задаётся на уровне таблицы через ALTER TABLE ... MODIFY TTL.
  - Первичный ключ / ORDER BY — основа для фильтрации; filter работает через WHERE.
  - Нет RETURNING после INSERT, поэтому create/create_all возвращают исходный объект домена.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from textwrap import indent
from typing import Any, Dict, Generic, List, Optional, TypeVar

from clickhouse_connect.driver.asyncclient import AsyncClient  # pip install clickhouse-connect
from pydantic import BaseModel

from service.ports.common.logs import logger
from service.ports.outbound.repo.fields import (
    ConditionOperation,
    FilterField,
    FilterFieldsConjunct,
    FilterFieldsDNF,
    PaginationQuery,
    UpdateFields,
)
from service.ports.outbound.repo.transaction import Transaction, TransactionFactory

TDomain = TypeVar("TDomain", bound=BaseModel)
TPK = TypeVar("TPK", bound=BaseModel)


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TTLRule:
    """
    Описывает правило TTL для ClickHouse-таблицы.

    Пример:
        TTLRule(column="created_at", interval_seconds=86400 * 30)
        → TTL created_at + INTERVAL 30 DAY

    Параметры:
        column            — колонка типа Date / DateTime, по которой считается TTL.
        interval_seconds  — время жизни строки в секундах.
        action            — что делать по истечении TTL:
                            None         → DELETE (по умолчанию в ClickHouse),
                            "DELETE"     → явное удаление,
                            "TO DISK 'cold'" → перемещение на другой диск/том,
                            "TO VOLUME 'archive'" → перемещение на другой том.
    """
    column: str
    interval_seconds: int
    action: Optional[str] = None  # None == DELETE

    # ---- helpers ----

    @property
    def _interval_expr(self) -> str:
        """Преобразуем секунды в наиболее крупную единицу без остатка."""
        s = self.interval_seconds
        for value, unit in (
            (86400 * 365, "YEAR"),
            (86400 * 30, "MONTH"),
            (86400 * 7, "WEEK"),
            (86400, "DAY"),
            (3600, "HOUR"),
            (60, "MINUTE"),
            (1, "SECOND"),
        ):
            if s % value == 0:
                return f"INTERVAL {s // value} {unit}"
        return f"INTERVAL {s} SECOND"

    def as_ttl_clause(self) -> str:
        """
        Возвращает TTL-выражение для использования в CREATE TABLE или ALTER TABLE.

        Например: ``TTL created_at + INTERVAL 30 DAY DELETE``
        """
        action_part = f" {self.action}" if self.action else ""
        return f"TTL {self.column} + {self._interval_expr}{action_part}"


# ---------------------------------------------------------------------------
# DDL — описание схемы таблицы
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    """
    Описание одной колонки таблицы ClickHouse.

    Параметры:
        name        — имя колонки.
        ch_type     — тип ClickHouse в виде строки, например:
                      "Int64", "String", "DateTime", "Nullable(String)",
                      "Enum8('pending'=1, 'done'=2)", "Array(Float64)".
        default     — DEFAULT-выражение (строка SQL), например:
                      "now()", "'pending'", "0". None — без DEFAULT.
        codec       — кодек сжатия, например "CODEC(Delta, LZ4)". None — по умолчанию.
        comment     — произвольный комментарий к колонке.

    Пример:
        ColumnDef("status_updated_at", "DateTime", default="now()")
        ColumnDef("description",       "Nullable(String)")
        ColumnDef("payload",           "String",  codec="CODEC(ZSTD(3))")
    """
    name: str
    ch_type: str
    default: Optional[str] = None
    codec: Optional[str] = None
    comment: Optional[str] = None

    def as_ddl(self) -> str:
        """Возвращает строку определения колонки для CREATE TABLE."""
        parts = [f"`{self.name}` {self.ch_type}"]
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        if self.comment is not None:
            safe = self.comment.replace("'", "\\'")
            parts.append(f"COMMENT '{safe}'")
        if self.codec is not None:
            parts.append(self.codec)
        return " ".join(parts)


@dataclass
class TableSchema:
    """
    Полное описание схемы таблицы ClickHouse.

    Параметры:
        columns         — список колонок (ColumnDef).
        order_by        — список колонок для ORDER BY (обязателен в MergeTree-движках).
        engine          — движок таблицы, например "MergeTree()", "ReplicatedMergeTree(...)".
                          По умолчанию "MergeTree()".
        partition_by    — выражение PARTITION BY, например "toYYYYMM(created_at)".
        primary_key     — список колонок PRIMARY KEY (если отличается от ORDER BY).
        settings        — словарь настроек таблицы (SETTINGS).
        cluster         — имя кластера для ON CLUSTER <cluster>.

    Пример:
        TableSchema(
            columns=[
                ColumnDef("task_run_id",       "Int64"),
                ColumnDef("status_updated_at", "DateTime"),
                ColumnDef("status",            "LowCardinality(String)"),
                ColumnDef("description",       "Nullable(String)"),
            ],
            order_by=["task_run_id", "status_updated_at"],
            partition_by="toYYYYMM(status_updated_at)",
        )
    """
    columns: List[ColumnDef]
    order_by: List[str]
    engine: str = "MergeTree()"
    partition_by: Optional[str] = None
    primary_key: Optional[List[str]] = None
    settings: Dict[str, Any] = field(default_factory=dict)
    cluster: Optional[str] = None

    def build_create_ddl(
        self,
        table_name: str,
        ttl_rule: Optional[TTLRule] = None,
        if_not_exists: bool = True,
    ) -> str:
        """
        Генерирует полный DDL-запрос CREATE TABLE.

        Параметры:
            table_name      — имя таблицы.
            ttl_rule        — правило TTL; если None — TTL-секция не добавляется.
            if_not_exists   — добавить IF NOT EXISTS (по умолчанию True).
        """
        guard = "IF NOT EXISTS " if if_not_exists else ""
        cluster = f" ON CLUSTER {self.cluster}" if self.cluster else ""

        col_lines = [col.as_ddl() for col in self.columns]
        cols_block = indent(",\n".join(col_lines), "    ")

        order_by_clause = ", ".join(self.order_by)

        parts = [
            f"CREATE TABLE {guard}`{table_name}`{cluster}",
            f"(\n{cols_block}\n)",
            f"ENGINE = {self.engine}",
        ]

        if self.partition_by:
            parts.append(f"PARTITION BY {self.partition_by}")

        parts.append(f"ORDER BY ({order_by_clause})")

        if self.primary_key:
            pk_clause = ", ".join(self.primary_key)
            parts.append(f"PRIMARY KEY ({pk_clause})")

        if ttl_rule:
            parts.append(ttl_rule.as_ttl_clause())

        if self.settings:
            setting_pairs = ", ".join(f"{k} = {v}" for k, v in self.settings.items())
            parts.append(f"SETTINGS {setting_pairs}")

        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Transaction-like context для ClickHouse
# ---------------------------------------------------------------------------

class CHSession(Transaction):
    """
    Лёгкая обёртка над AsyncClient для единообразия с SA-слоем.

    ClickHouse не поддерживает ACID-транзакции, но позволяет
    группировать операции через один клиент-объект.
    """

    async def begin(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        pass

    def __init__(self, client: AsyncClient) -> None:
        self.client = client

class CHSessionFactory(TransactionFactory):
    def __init__(self, client: AsyncClient):
        self.client = client

    def create(self) -> CHSession:
        return CHSession(self.client)


# ---------------------------------------------------------------------------
# Абстрактный репозиторий
# ---------------------------------------------------------------------------

class AbstractCHRepo(ABC, Generic[TDomain, TPK]):
    """
    Базовый репозиторий для ClickHouse.

    Субклассам обязательно реализовать:
        table_name    (property) — имя таблицы.
        table_schema  (property) — схема таблицы (TableSchema).
        to_domain(row: dict) -> TDomain
        to_row(obj: TDomain) -> dict
        pk_to_where(pk: TPK) -> (str, dict)  ← WHERE-фрагмент и параметры

    column_names выводится автоматически из table_schema.columns.

    Опционально переопределить:
        ttl_rule -> Optional[TTLRule]  ← если нужен TTL
    """

    def __init__(
        self,
        client: AsyncClient,
        chunk_size: int = 50_000,
    ) -> None:
        self._client = client
        self._chunk_size = chunk_size

    # ------------------------------------------------------------------
    # Абстрактные методы — должны быть реализованы в каждом репозитории
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Имя таблицы в ClickHouse."""

    @property
    @abstractmethod
    def table_schema(self) -> TableSchema:
        """
        Схема таблицы. Используется для автоматического CREATE TABLE
        и для определения column_names при INSERT.

        Пример:
            @property
            def table_schema(self) -> TableSchema:
                return TableSchema(
                    columns=[
                        ColumnDef("task_run_id",       "Int64"),
                        ColumnDef("status_updated_at", "DateTime"),
                        ColumnDef("status",            "LowCardinality(String)"),
                        ColumnDef("description",       "Nullable(String)"),
                    ],
                    order_by=["task_run_id", "status_updated_at"],
                    partition_by="toYYYYMM(status_updated_at)",
                )
        """

    @abstractmethod
    def to_domain(self, row: Dict[str, Any]) -> TDomain:
        """Конвертирует словарь строки ClickHouse в объект домена."""

    @abstractmethod
    def to_row(self, obj: TDomain) -> Dict[str, Any]:
        """Конвертирует объект домена в словарь для вставки в ClickHouse."""

    @abstractmethod
    def pk_to_where(self, pk: TPK) -> tuple[str, Dict[str, Any]]:
        """
        Возвращает пару (where_clause, params) для фильтрации по PK.

        Пример:
            return (
                "task_run_id = {task_run_id:Int64} AND status_updated_at = {ts:DateTime}",
                {"task_run_id": pk.task_run_id, "ts": pk.status_updated_at},
            )
        """

    # ------------------------------------------------------------------
    # column_names выводится из схемы — переопределять не нужно
    # ------------------------------------------------------------------

    @property
    def column_names(self) -> List[str]:
        """Список имён колонок в том же порядке, что table_schema.columns."""
        return [col.name for col in self.table_schema.columns]

    # ------------------------------------------------------------------
    # DDL — создание таблицы
    # ------------------------------------------------------------------

    def get_create_table_ddl(self, if_not_exists: bool = True) -> str:
        """
        Возвращает DDL-строку CREATE TABLE для данного репозитория.

        Удобно для логирования, миграций или ручного применения.
        TTL из ttl_rule (если задан) включается в DDL автоматически.
        """
        return self.table_schema.build_create_ddl(
            table_name=self.table_name,
            ttl_rule=self.ttl_rule,
            if_not_exists=if_not_exists,
        )

    async def create_table_if_not_exists(self) -> None:
        """
        Создаёт таблицу в ClickHouse, если она ещё не существует.

        Вызывайте при старте приложения или в миграциях.
        Безопасно вызывать повторно — IF NOT EXISTS защищает от ошибки.

        TTL из ttl_rule (если задан) вшивается прямо в CREATE TABLE,
        поэтому отдельный вызов apply_ttl() после этого не нужен.
        """
        ddl = self.get_create_table_ddl(if_not_exists=True)
        logger.warning(f"Creating table if not exists:\n {ddl}")
        await self._client.command(ddl)

    # ------------------------------------------------------------------
    # TTL — переопределите, если нужен
    # ------------------------------------------------------------------

    @property
    def ttl_rule(self) -> Optional[TTLRule]:
        """
        Правило TTL для таблицы.
        Если None — TTL не применяется / не меняется.

        Пример переопределения в наследнике:
            @property
            def ttl_rule(self) -> Optional[TTLRule]:
                return TTLRule(column="status_updated_at", interval_seconds=86400 * 90)
        """
        return None

    async def apply_ttl(self) -> None:
        """
        Применяет TTL-правило к таблице через ALTER TABLE ... MODIFY TTL.

        Вызывайте при старте приложения или в миграциях.
        Безопасно вызывать повторно — ClickHouse идемпотентно обновит выражение.

        Поднимает ValueError, если ttl_rule не задан.
        """
        rule = self.ttl_rule
        if rule is None:
            raise ValueError(
                f"TTL rule is not defined for {self.__class__.__name__}. "
                "Override the `ttl_rule` property."
            )
        ddl = f"ALTER TABLE {self.table_name} MODIFY {rule.as_ttl_clause()}"
        logger.info("Applying TTL: %s", ddl)
        await self._client.command(ddl)

    async def remove_ttl(self) -> None:
        """Удаляет TTL с таблицы (ALTER TABLE ... REMOVE TTL)."""
        ddl = f"ALTER TABLE {self.table_name} REMOVE TTL"
        logger.info("Removing TTL from %s", self.table_name)
        await self._client.command(ddl)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(
        self,
        obj: TDomain,
        transaction: Optional[CHSession] = None,
    ) -> TDomain:
        """
        Вставляет одну запись.

        ClickHouse не поддерживает RETURNING, поэтому возвращаем
        переданный объект домена (без перечитывания из БД).
        """
        row = self.to_row(obj)
        client = self._client
        await client.insert(
            table=self.table_name,
            data=[list(row[c] for c in self.column_names)],
            column_names=self.column_names,
        )
        return obj

    async def create_all(
        self,
        objs: List[TDomain],
        transaction: Optional[CHSession] = None,
    ) -> List[TDomain]:
        """
        Batch-вставка списка объектов.

        Данные разбиваются на чанки по self._chunk_size для предотвращения
        перегрузки сервера при больших объёмах.
        """
        if not objs:
            return []

        client = self._client

        for i in range(0, len(objs), self._chunk_size):
            chunk = objs[i: i + self._chunk_size]
            rows = [self.to_row(obj) for obj in chunk]
            data = [[row.get(c) for c in self.column_names] for row in rows]
            await client.insert(
                table=self.table_name,
                data=data,
                column_names=self.column_names,
            )

        return objs

    async def get(
        self,
        pk: TPK,
        transaction: Optional[CHSession] = None,
    ) -> Optional[TDomain]:
        """Возвращает одну запись по первичному ключу или None."""
        where, params = self.pk_to_where(pk)
        query = f"SELECT * FROM {self.table_name} WHERE {where} LIMIT 1"
        client = self._client
        result = await client.query(query, parameters=params)
        rows = result.named_results()
        if not rows:
            return None
        return self.to_domain(rows[0])

    async def get_all(
        self,
        transaction: Optional[CHSession] = None,
    ) -> List[TDomain]:
        """Возвращает все записи таблицы."""
        query = f"SELECT * FROM {self.table_name}"
        client = self._client
        result = await client.query(query)
        return [self.to_domain(row) for row in result.named_results()]

    async def update(
        self,
        pk: TPK,
        fields: UpdateFields,
        transaction: Optional[CHSession] = None,
    ) -> None:
        """
        Обновляет поля записи через ALTER TABLE ... UPDATE (мутация ClickHouse).

        ВАЖНО: мутации в ClickHouse выполняются асинхронно на уровне сервера.
        Метод лишь инициирует мутацию и не ждёт её завершения.
        Для ожидания используйте wait_for_mutations() или system.mutations.
        """
        where, params = self.pk_to_where(pk)
        update_dict = fields.to_dict()

        set_parts = []
        for col, val in update_dict.items():
            placeholder = f"upd_{col}"
            set_parts.append(f"{col} = {{{placeholder}:{_ch_type_hint(val)}}}")
            params[placeholder] = val

        set_clause = ", ".join(set_parts)
        query = f"ALTER TABLE {self.table_name} UPDATE {set_clause} WHERE {where}"
        client = self._client
        await client.command(query, parameters=params)

    async def delete(
        self,
        pk: TPK,
        transaction: Optional[CHSession] = None,
    ) -> None:
        """
        Удаляет запись по PK через ALTER TABLE ... DELETE (мутация).

        Так же, как update, выполняется асинхронно на стороне сервера.
        """
        where, params = self.pk_to_where(pk)
        query = f"ALTER TABLE {self.table_name} DELETE WHERE {where}"
        client = self._client
        await client.command(query, parameters=params)

    async def delete_by_condition(
        self,
        filter_fields_dnf: FilterFieldsDNF,
        transaction: Optional[CHSession] = None,
    ) -> None:
        """
        Удаляет записи по произвольному условию (мутация ClickHouse).

        Используйте с осторожностью — мутации дороги.
        Для массовой очистки старых данных предпочтительнее TTL.
        """
        where, params = _dnf_to_where(filter_fields_dnf)
        query = f"ALTER TABLE {self.table_name} DELETE WHERE {where}"
        client = self._client
        await client.command(query, parameters=params)

    async def filter(
        self,
        filter_fields_dnf: FilterFieldsDNF,
        transaction: Optional[CHSession] = None,
    ) -> List[TDomain]:
        """Возвращает записи, удовлетворяющие условию в DNF-форме."""
        where, params = _dnf_to_where(filter_fields_dnf)
        query = f"SELECT * FROM {self.table_name} WHERE {where}"
        client = self._client
        result = await client.query(query, parameters=params)
        return [self.to_domain(row) for row in result.named_results()]

    async def count_by_fields(
        self,
        filter_fields_dnf: FilterFieldsDNF,
        transaction: Optional[CHSession] = None,
    ) -> int:
        """Возвращает количество записей, удовлетворяющих условию."""
        where, params = _dnf_to_where(filter_fields_dnf)
        query = f"SELECT count() FROM {self.table_name} WHERE {where}"
        client = self._client
        result = await client.query(query, parameters=params)
        return result.first_row[0]

    async def paginated(
        self,
        pagination_query: PaginationQuery,
        transaction: Optional[CHSession] = None,
    ) -> List[TDomain]:
        """
        Постраничная выборка с фильтрацией, сортировкой и пагинацией.

        Параметры PaginationQuery используются так же, как в SA-реализации.
        """
        parts = [f"SELECT * FROM {self.table_name}"]
        params: Dict[str, Any] = {}

        if pagination_query.filter_fields_dnf:
            where, where_params = _dnf_to_where(pagination_query.filter_fields_dnf)
            parts.append(f"WHERE {where}")
            params.update(where_params)

        if pagination_query.order_by:
            direction = "ASC" if pagination_query.asc_sort else "DESC"
            parts.append(f"ORDER BY {pagination_query.order_by} {direction}")

        if pagination_query.limit_per_page:
            parts.append(f"LIMIT {int(pagination_query.limit_per_page)}")

        if pagination_query.offset_page:
            parts.append(f"OFFSET {int(pagination_query.offset_page)}")

        query = " ".join(parts)
        client = self._client
        result = await client.query(query, parameters=params)
        return [self.to_domain(row) for row in result.named_results()]

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------

    async def wait_for_mutations(self, timeout_seconds: int = 60) -> None:
        """
        Блокирует выполнение до завершения всех мутаций на таблице
        или до истечения таймаута.

        Используется после update() / delete() / delete_by_condition()
        когда важна согласованность.
        """
        import asyncio
        query = (
            "SELECT count() FROM system.mutations "
            "WHERE table = {table:String} AND is_done = 0"
        )
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            result = await self._client.query(query, parameters={"table": self.table_name})
            pending = result.first_row[0]
            if pending == 0:
                return
            await asyncio.sleep(0.5)
        raise TimeoutError(
            f"Mutations on {self.table_name} did not complete within {timeout_seconds}s"
        )


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def _ch_type_hint(value: Any) -> str:
    """
    Возвращает подсказку типа для параметризованных запросов ClickHouse.

    Порядок проверок важен:
      - bool до int, потому что bool является подклассом int в Python.
      - datetime до date, потому что datetime является подклассом date.
      - list/tuple → рекурсивно определяем тип элемента по первому непустому значению.
    """
    from datetime import date, datetime

    if isinstance(value, bool):
        return "UInt8"
    if isinstance(value, int):
        return "Int64"
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, datetime):
        return "DateTime"
    if isinstance(value, date):
        return "Date"
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
        # Берём первый непустой элемент для определения типа
        first = next((v for v in items if v is not None), None)
        elem_type = _ch_type_hint(first) if first is not None else "String"
        return f"Array({elem_type})"
    return "String"


def _dnf_to_where(
        dnf: FilterFieldsDNF,
) -> tuple[str, Dict[str, Any]]:
    """
    Преобразует FilterFieldsDNF в пару (WHERE-строка, словарь параметров)
    для параметризованных запросов clickhouse-connect.

    Плейсхолдеры имеют вид {param_name:TypeHint}.
    Имена параметров гарантированно уникальны через счётчик.
    """
    params: Dict[str, Any] = {}
    counter = [0]  # мутируемый счётчик для вложенных функций

    def next_name(col: str) -> str:
        counter[0] += 1
        return f"p_{col}_{counter[0]}"

    def field_to_sql(f: FilterField) -> str:
        col = f.name
        op = f.operation

        if op == ConditionOperation.IS_NULL:
            return f"isNull({col})"
        if op == ConditionOperation.NOT_NULL:
            return f"isNotNull({col})"

        if op in (ConditionOperation.IN, ConditionOperation.NOT_IN):
            items = list(f.value)
            pname = next_name(col)
            params[pname] = items
            # Определяем тип элементов по первому непустому значению в списке
            array_type = _ch_type_hint(items)
            keyword = "IN" if op == ConditionOperation.IN else "NOT IN"
            return f"{col} {keyword} {{{pname}:{array_type}}}"

        if op == ConditionOperation.CONTAINS:
            pname = next_name(col)
            params[pname] = str(f.value)
            return f"ilike(toString({col}), {{{pname}:String}})"

        pname = next_name(col)
        params[pname] = f.value
        type_hint = _ch_type_hint(f.value)

        op_map = {
            ConditionOperation.EQ: "=",
            ConditionOperation.NE: "!=",
            ConditionOperation.GT: ">",
            ConditionOperation.GTE: ">=",
            ConditionOperation.LT: "<",
            ConditionOperation.LTE: "<=",
        }
        sql_op = op_map.get(op)
        if sql_op is None:
            raise RuntimeError(f"Unknown ConditionOperation: {op}")
        return f"{col} {sql_op} {{{pname}:{type_hint}}}"

    def conjunct_to_sql(c: FilterFieldsConjunct) -> str:
        parts = [field_to_sql(f) for f in c.group]
        return "(" + " AND ".join(parts) + ")"

    disjuncts = [conjunct_to_sql(c) for c in dnf.conjunctions]
    where = " OR ".join(disjuncts) if disjuncts else "1"
    return where, params
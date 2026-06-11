# service/domain/use_cases/internal/compress_task_progress.py

from collections import defaultdict
from datetime import timedelta
from typing import Dict, List

from service.domain.schemas.task_progress import TimeIntervalTaskProgressPK, TimeIntervalTaskProgress
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation
from service.ports.outbound.repo.transaction import TransactionFactory


class CompressTaskProgressUCRq(UCRequest):
    max_gap_seconds: float = 30.0
    task_ids: List[int] | None = None  # None — обрабатывать все задачи


class CompressTaskProgressUCRs(UCResponse):
    request: CompressTaskProgressUCRq
    tasks_processed: int
    records_before: int
    records_after: int
    records_removed: int


class CompressTaskProgressUC(UseCase):
    """
    Сжимает соседние записи TimeIntervalTaskProgress в одну, если разрыв
    между правой границей предыдущей записи и левой границей следующей
    меньше max_gap_seconds.

    Алгоритм для каждой задачи:
      1. Берём все записи, сортируем по right_bound_at.
      2. Идём слева направо, накапливаем "текущий объединённый интервал".
      3. Если gap = next.left_bound_at - current.right_bound_at <= max_gap,
         сливаем: left_bound_at = min(left), right_bound_at = max(right),
         collected/saved суммируются.
      4. Иначе — фиксируем текущий объединённый интервал и начинаем новый.
      5. В конце: удаляем старые записи и вставляем объединённые
         (если объединение реально что-то сжало).
    """

    def __init__(
        self,
        time_interval_task_progress_repo: Repo[
            TimeIntervalTaskProgress, TimeIntervalTaskProgress, TimeIntervalTaskProgressPK
        ],
        transaction_factory: TransactionFactory,
    ):
        self._repo = time_interval_task_progress_repo
        self._transaction_factory = transaction_factory

    async def apply(self, request: CompressTaskProgressUCRq) -> CompressTaskProgressUCRs:
        # ── 1. Загружаем записи ──────────────────────────────────────────────
        if request.task_ids:
            records = await self._repo.filter(
                FilterFieldsDNF.single("task_id", request.task_ids, ConditionOperation.IN)
            )
        else:
            records = await self._repo.get_all()

        records_before = len(records)
        if not records:
            return CompressTaskProgressUCRs(
                success=True, request=request,
                tasks_processed=0, records_before=0, records_after=0, records_removed=0,
            )

        # ── 2. Группируем по task_id ─────────────────────────────────────────
        by_task: Dict[int, List[TimeIntervalTaskProgress]] = defaultdict(list)
        for r in records:
            by_task[r.task_id].append(r)

        max_gap = timedelta(seconds=request.max_gap_seconds)

        to_delete: List[TimeIntervalTaskProgressPK] = []
        to_create: List[TimeIntervalTaskProgress] = []
        records_after = 0

        # ── 3. Сжимаем по каждой задаче ──────────────────────────────────────
        for task_id, recs in by_task.items():
            merged = self._compress_task(task_id, recs, max_gap)
            records_after += len(merged)

            # Если число записей не изменилось — сжимать нечего,
            # не трогаем эту задачу вообще
            if len(merged) == len(recs):
                continue

            for r in recs:
                to_delete.append(TimeIntervalTaskProgressPK(
                    task_id=r.task_id, right_bound_at=r.right_bound_at,
                ))
            to_create.extend(merged)

        # ── 4. Применяем изменения одной транзакцией ─────────────────────────
        if to_delete:
            async with self._transaction_factory.create() as transaction:
                for pk in to_delete:
                    await self._repo.delete(pk, transaction=transaction)
                await self._repo.create_all(to_create, transaction=transaction)

        tasks_processed = sum(
            1 for task_id, recs in by_task.items()
            if len(self._compress_task(task_id, recs, max_gap)) != len(recs)
        ) if to_delete else 0

        logger.info(
            f"CompressTaskProgressUC: задач обработано={tasks_processed}, "
            f"записей было={records_before}, стало={records_before - len(to_delete) + len(to_create)}, "
            f"удалено={len(to_delete) - len(to_create)}"
        )

        return CompressTaskProgressUCRs(
            success=True,
            request=request,
            tasks_processed=tasks_processed,
            records_before=records_before,
            records_after=records_before - len(to_delete) + len(to_create),
            records_removed=len(to_delete) - len(to_create),
        )

    @staticmethod
    def _compress_task(
        task_id: int,
        records: List[TimeIntervalTaskProgress],
        max_gap: timedelta,
    ) -> List[TimeIntervalTaskProgress]:
        """Сжимает записи одной задачи. Записи без right_bound_at игнорируются."""

        # Сортируем по right_bound_at; записи без left_bound_at не участвуют
        # в слиянии (нечего проверять на разрыв) — оставляем как есть отдельно.
        sortable = sorted(records, key=lambda r: r.right_bound_at)

        merged: List[TimeIntervalTaskProgress] = []
        current: TimeIntervalTaskProgress | None = None

        for r in sortable:
            if current is None:
                current = r.model_copy(deep=True)
                continue

            if current.left_bound_at is None or r.left_bound_at is None:
                # Нет данных о левой границе — не сливаем, фиксируем текущий
                merged.append(current)
                current = r.model_copy(deep=True)
                continue

            gap = r.left_bound_at - current.right_bound_at

            if gap <= max_gap:
                # Сливаем r в current
                current = TimeIntervalTaskProgress(
                    task_id=task_id,
                    left_bound_at=min(current.left_bound_at, r.left_bound_at),
                    right_bound_at=max(current.right_bound_at, r.right_bound_at),
                    collected_data_amount=current.collected_data_amount + r.collected_data_amount,
                    saved_data_amount=current.saved_data_amount + r.saved_data_amount,
                )
            else:
                merged.append(current)
                current = r.model_copy(deep=True)

        if current is not None:
            merged.append(current)

        return merged
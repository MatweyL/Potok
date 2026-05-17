from typing import Dict, List

from sqlalchemy import select

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.domain.schemas.task_progress import TimeIntervalTaskProgress, TimeIntervalTaskProgressPK


class SATimeIntervalTaskProgressRepo(AbstractSARepo):
    def to_model(self, obj: TimeIntervalTaskProgress) -> models.TimeIntervalTaskProgress:
        return models.TimeIntervalTaskProgress(task_id=obj.task_id,
                                               right_bound_at=obj.right_bound_at,
                                               left_bound_at=obj.left_bound_at,
                                               collected_data_amount=obj.collected_data_amount,
                                               saved_data_amount=obj.saved_data_amount)

    def to_domain(self, obj: models.TimeIntervalTaskProgress) -> TimeIntervalTaskProgress:
        return TimeIntervalTaskProgress(task_id=obj.task_id,
                                        right_bound_at=obj.right_bound_at,
                                        left_bound_at=obj.left_bound_at,
                                        collected_data_amount=obj.collected_data_amount,
                                        saved_data_amount=obj.saved_data_amount)

    def pk_to_model_pk(self, pk: TimeIntervalTaskProgressPK) -> Dict:
        return {
            "task_id": pk.task_id,
            "right_bound_at": pk.right_bound_at
        }


    async def get_by_task_ids_ordered(self, task_ids: List[int]) -> List[TimeIntervalTaskProgress]:
        if not task_ids:
            return []
        query = (
            select(self._model_class)
            .where(self._model_class.task_id.in_(task_ids))
            .order_by(self._model_class.task_id, self._model_class.right_bound_at.desc())
        )
        async with self._database.session as session:
            result = await session.scalars(query)
            return [self.to_domain(model) for model in result.all()]

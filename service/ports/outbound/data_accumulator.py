import asyncio
from datetime import datetime
from typing import List

from service.ports.common.changeable_parameter import ChangeableFloatParameter, ChangeableIntParameter
from service.ports.common.logs import logger
from service.ports.outbound.repo.abstract import Repo


class DataAccumulator:
    def __init__(self,
                 repo: Repo,
                 upload_timeout: ChangeableFloatParameter,
                 max_batch_size_to_upload: ChangeableIntParameter, ):
        self._repo = repo
        self._upload_timeout = upload_timeout
        self._max_batch_size_to_upload = max_batch_size_to_upload
        self._batch = set()
        self._last_upload_at = datetime.min
        self._name = self._repo.__class__.__name__ + ":Accumulator"
        logger.info(f"[{self._name}] initialized")

    def add(self, item):
        self._batch.add(item)

    def add_all(self, items: List):
        self._batch.update(items)

    async def upload(self):
        max_batch_size_reached = len(self._batch) >= self._max_batch_size_to_upload.value
        upload_timeout_reached = (datetime.now() - self._last_upload_at).total_seconds() > self._upload_timeout.value
        if self._batch and (max_batch_size_reached or upload_timeout_reached):
            batch = self._batch
            self._batch.clear()
            logger.info(f"[{self._name}] Starting data upload. "
                        f"Reason: {max_batch_size_reached=}, {upload_timeout_reached=}")
            await self._repo.create_all(batch)
            logger.info(f"[{self._name}] Data uploaded successfully. Batch size: {len(batch)}")
            self._last_upload_at = datetime.now()


class DataAccumulatorRunner:
    def __init__(self, data_accumulators: List[DataAccumulator]):
        self._data_accumulators = data_accumulators

    async def run(self):
        coroutines = [data_accumulator.upload() for data_accumulator in self._data_accumulators]
        await asyncio.gather(*coroutines)

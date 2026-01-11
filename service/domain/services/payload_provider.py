from collections import defaultdict
from typing import List, Dict, Optional

from service.domain.schemas.payload import PayloadPK, Payload
from service.domain.schemas.task import Task
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


class PayloadProvider:
    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK]):
        self._payload_repo = payload_repo

    async def provide(self, tasks: List[Task]) -> Dict[Task, Optional[Payload]]:
        if not tasks:
            return {}
        tasks_by_payload_id = defaultdict(list)
        for task in tasks:
            tasks_by_payload_id[task.payload_id].append(task)
        payload_ids = tasks_by_payload_id.keys()
        payloads = await self._payload_repo.filter(FilterFieldsDNF.single("id",
                                                                          payload_ids,
                                                                          ConditionOperation.IN))
        payload_by_id = {payload.id: payload for payload in payloads}
        payload_by_task = {}
        for payload_id, tasks in tasks_by_payload_id.items():
            payload = payload_by_id.get(payload_id)
            for task in tasks:
                payload_by_task[task] = payload
        return payload_by_task

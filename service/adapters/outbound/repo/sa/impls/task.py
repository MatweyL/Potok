from typing import Dict

from sqlalchemy import text

from service.adapters.outbound.repo.sa import models
from service.adapters.outbound.repo.sa.abstract import AbstractSARepo
from service.adapters.outbound.repo.sa.database import Database
from service.adapters.outbound.repo.sa.impls.task_mapper import TaskMapper
from service.domain.schemas.task import TaskPK, Task
from service.ports.outbound.repo.task import TaskProvider, TasksToTransitStatus, TaskStatisticsProvider, \
    TaskGroupsStatistics


class SATaskRepo(AbstractSARepo):
    def to_model(self, obj: Task) -> models.Task:
        return TaskMapper.to_model(obj)

    def to_domain(self, obj: models.Task) -> Task:
        return TaskMapper.to_domain(obj)

    def pk_to_model_pk(self, pk: TaskPK) -> Dict:
        return {"id": pk.id}


class SATaskProvider(TaskProvider):
    def __init__(self, database: Database, ):
        self._database = database

    async def provide_tasks_ids_to_transit_via_sql(self) -> TasksToTransitStatus:
        async with self._database.session as session:
            query = text("""WITH last_three_runs AS (
    SELECT 
        tr.task_id,
        tr.status,
        ROW_NUMBER() OVER (
            PARTITION BY tr.task_id 
            ORDER BY tr.status_updated_at DESC
        ) AS rn
    FROM task_run tr
    JOIN task t ON t.id = tr.task_id AND t.status = 'EXECUTION'
    WHERE tr.status IN ('SUCCEED', 'ERROR')
    -- Исключаем task_id у которых есть хоть один "посторонний" статус
    AND NOT EXISTS (
        SELECT 1 FROM task_run tr2
        WHERE tr2.task_id = tr.task_id
          AND tr2.status NOT IN ('SUCCEED', 'ERROR')
    )
)
SELECT 
    task_id,
    CASE 
        WHEN BOOL_OR(status = 'SUCCEED') THEN 'SUCCEED'
        WHEN BOOL_AND(status = 'ERROR')  THEN 'ERROR'
    END AS final_status
FROM last_three_runs
WHERE rn <= 3
GROUP BY task_id
HAVING BOOL_OR(status = 'SUCCEED') OR BOOL_AND(status = 'ERROR')
                """)

            result = await session.execute(query)
            rows = result.fetchall()

            succeed_ids = [row.task_id for row in rows if row.final_status == 'SUCCEED']
            error_ids = [row.task_id for row in rows if row.final_status == 'ERROR']

            return TasksToTransitStatus(succeed_ids=succeed_ids,
                                        error_ids=error_ids)



class SATaskStatisticsProvider(TaskStatisticsProvider):
    def __init__(self, database: Database, ):
        self._database = database

    async def provide_groups_statistics(self) -> TaskGroupsStatistics:
        async with self._database.session as session:
            query = text("""
                WITH tasks_count_by_group AS (
                    SELECT group_id, COUNT(*) AS amount
                    FROM task
                    GROUP BY group_id
                )
                SELECT tg.name AS group_name, COALESCE(tcbg.amount, 0) AS amount
                FROM task_group tg
                LEFT JOIN tasks_count_by_group tcbg ON tcbg.group_id = tg.id
            """)
            result = await session.execute(query)
            rows = result.fetchall()

            tasks_count_by_group_name: Dict[str, int] = {}
            total_tasks_count = 0

            for row in rows:
                group_name = row[0]
                amount = int(row[1])
                tasks_count_by_group_name[group_name] = amount
                total_tasks_count += amount

            return TaskGroupsStatistics(
                total_tasks_count=total_tasks_count,
                tasks_count_by_group_name=tasks_count_by_group_name,
            )

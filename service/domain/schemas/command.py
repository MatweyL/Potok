from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from service.domain.schemas.enums import CommandType, TaskRunStatus
from service.domain.schemas.execution_results import ExecutionResults
from service.domain.schemas.task_run import TaskRun


class Command(BaseModel):
    """
    Объект, отправляемый в очередь для выполнения.
    Содержит в себе всю необходимую информацию для выполнения задачи
    """
    type: CommandType = CommandType.EXECUTE
    task_run: TaskRun


class CommandResponse(BaseModel):
    command: Command
    status: TaskRunStatus
    description: Optional[str] = None
    result: Optional[ExecutionResults] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

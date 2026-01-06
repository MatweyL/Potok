import enum


class PriorityType(str, enum.Enum):
    LOWEST = "LOWEST"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    HIGHEST = "HIGHEST"


class TaskStatus(str, enum.Enum):
    NEW = "NEW"  # Задача создана
    EXECUTION = "EXECUTION"  # Задача выполняется
    SUCCEED = "SUCCEED"  # Выполнение задачи успешно завершено
    FINISHED = "FINISHED"  # Задача завершена окончательно и больше не подлежит выполнению

    CANCELLED = "CANCELLED"  # Выполнение задачи отменено пользователем
    ERROR = "ERROR"  # Все запуски задачи получили ошибку ERROR


class TaskRunStatus(str, enum.Enum):
    WAITING = "WAITING"  # Задача ожидает выполнения
    QUEUED = "QUEUED"  # Задача отправлена в очередь
    EXECUTION = "EXECUTION"  # Задача в процессе выполнения
    INTERRUPTED = "INTERRUPTED"  # Выполнение задачи прервано
    TEMP_ERROR = "TEMP_ERROR"  # Выполнение завершилось с временной ошибкой, задачу можно попытаться выполнить снова
    CANCELLED = "CANCELLED"  # Выполнение задачи отменено пользователем
    ERROR = "ERROR"  # Задача определена как ошибочная и больше не может быть выполнена повторно
    SUCCEED = "SUCCEED"  # Задача успешно выполнена


class TaskType(str, enum.Enum):
    UNDEFINED = "UNDEFINED"
    TIME_INTERVAL = "TIME_INTERVAL"
    PAGINATION = "PAGINATION"


class CommandType(str, enum.Enum):
    CANCEL = "CANCEL"  # Отменить выполнение задачи
    EXECUTE = "EXECUTE"  # Выполнить задачу


class MonitoringAlgorithmType(str, enum.Enum):
    PERIODIC = "PERIODIC"
    SINGLE = "SINGLE"

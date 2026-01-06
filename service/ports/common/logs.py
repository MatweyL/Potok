import logging
import sys
from typing import Optional

APP_NAME = "potok"
logger = logging.getLogger(APP_NAME)


def setup_logging(
        level: int = logging.WARNING,
        format_string: Optional[str] = None,
        stream=None,
        log_file: Optional[str] = None
) -> None:
    """
    Настройка логирования.

    Args:
        level: Уровень логирования (по умолчанию WARNING)
        format_string: Формат строки лога
        stream: Поток для вывода логов (по умолчанию stderr)
        log_file: Путь к файлу для записи логов (опционально)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s'

    # Очищаем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Создаем обработчик для stderr или указанного потока
    if stream is None:
        stream = sys.stderr

    handlers = []

    # Консольный обработчик
    console_handler = logging.StreamHandler(stream)
    console_handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # Файловый обработчик (если указан)
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except Exception as e:
            logger.error(f"Не удалось создать файловый обработчик: {e}")

    # Добавляем обработчики к логгеру
    for handler in handlers:
        logger.addHandler(handler)

    # Устанавливаем уровень
    logger.setLevel(level)

    # Предотвращаем передачу логов корневому логгеру
    logger.propagate = False

    logger.debug("Логирование инициализировано для %s", APP_NAME)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Получить дочерний логгер для конкретного модуля.

    Args:
        name: Имя дочернего логгера (обычно __name__ модуля)

    Returns:
        Дочерний логгер с наследованием настроек
    """
    if name:
        return logger.getChild(name)
    return logger


# Функции для удобного управления уровнем логирования
def set_log_level(level: int) -> None:
    """Установить уровень логирования."""
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


def disable_logging() -> None:
    """Отключить логирование."""
    logger.setLevel(logging.CRITICAL + 1)


def enable_logging(level: int = logging.WARNING) -> None:
    """Включить логирование."""
    logger.setLevel(level)


# Добавим полезные методы для работы с логгером
def add_handler(handler: logging.Handler) -> None:
    """Добавить кастомный обработчик к логгеру."""
    logger.addHandler(handler)


def remove_handler(handler: logging.Handler) -> None:
    """Удалить обработчик из логгера."""
    logger.removeHandler(handler)


# Инициализация логирования при импорте
setup_logging()

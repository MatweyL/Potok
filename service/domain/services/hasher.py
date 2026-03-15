from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError


class Hasher:
    """
    Безопасный хэшер паролей на основе Argon2id (рекомендуемый алгоритм 2025–2026 гг.)

    Использует библиотеку argon2-cffi — самую актуальную и хорошо поддерживаемую реализацию Argon2 в Python.
    """

    def __init__(self):
        # Рекомендуемые параметры на 2026 год для большинства серверов
        # ~ 100–300 мс на хэширование на современном CPU
        self.ph = PasswordHasher(
            time_cost=2,          # итерации (время)
            memory_cost=102400,   # 100 MiB памяти — хороший баланс
            parallelism=8,        # потоки (зависит от ядер CPU)
            hash_len=32,          # длина хэша
            salt_len=16,          # длина соли
            encoding='utf-8',
        )

    def hash(self, text: str) -> str:
        """
        Хэширует пароль и возвращает строку в формате PHC (argon2id$v=19$m=102400,t=2,p=8$...)

        Автоматически генерирует уникальную соль для каждого вызова.
        """
        if not text or not isinstance(text, str):
            raise ValueError("Password must be a non-empty string")
        return self.ph.hash(text)

    def verify(self, plain_text: str, hashed: str) -> bool:
        """
        Проверяет, соответствует ли открытый пароль хэшу.

        Возвращает True при успехе, False при несовпадении.
        Выбрасывает исключения только при критических ошибках формата.
        """
        if not plain_text or not hashed:
            return False

        try:
            return self.ph.verify(hashed, plain_text)
        except VerifyMismatchError:
            return False
        except InvalidHashError:
            # Некорректный формат хэша → считаем невалидным
            return False

    def needs_rehash(self, hashed: str) -> bool:
        """
        Проверяет, нужно ли перехэшировать пароль (например, если параметры устарели)
        """
        try:
            return self.ph.check_needs_rehash(hashed)
        except InvalidHashError:
            return True
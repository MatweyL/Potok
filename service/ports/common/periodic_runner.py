import asyncio
from asyncio import Task
from typing import Callable, Awaitable, Any, Union

from service.ports.common.changeable_parameter import ChangeableFloatParameter
from service.ports.common.logs import logger


class PeriodicRunner:

    def __init__(self,
                 method: Callable[[], Union[Any, Awaitable]],
                 timeout: float | ChangeableFloatParameter,
                 before_first_run_timeout: int = 0,
                 run_name: str = None,
                 verbose_exception: bool = False, ):
        self._run_name = run_name or method.__name__
        self._timeout = ChangeableFloatParameter(name=self._run_name,
                                                 value=timeout) if isinstance(timeout, (float, int)) else timeout
        self._method = method
        self._before_first_run_timeout = before_first_run_timeout
        self._verbose_exception = verbose_exception

        self._is_coroutine_function = asyncio.iscoroutinefunction(self._method)

        self._task: Task = None

    @property
    def timeout(self) -> ChangeableFloatParameter:
        return self._timeout

    def cancel(self):
        self._task.cancel()

    def create_periodic_task(self) -> Task:
        self._task = asyncio.create_task(self.run_periodically())
        return self._task

    async def run_periodically(self):
        logger.debug(f'will run periodically {self._run_name}')
        if self._before_first_run_timeout > 0:
            logger.debug(f"will sleep {self._before_first_run_timeout} before first run {self._run_name}")
            await asyncio.sleep(self._before_first_run_timeout)
        else:
            logger.debug(f"no sleep before first run {self._run_name}")
        while True:
            try:
                if self._is_coroutine_function:
                    await self._method()
                else:
                    self._method()
            except BaseException as e:
                logger.error(f"got error {e.__class__.__name__}: {e} in periodically running function {self._run_name}")
                if self._verbose_exception:
                    logger.exception(e)
            logger.debug(f"will sleep {self._timeout.value} before run {self._run_name}")
            await asyncio.sleep(self._timeout.value)

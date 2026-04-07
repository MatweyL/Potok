from typing import List, Iterable, Dict

from service.domain.use_cases.abstract import UseCase


class UseCaseMetricCollector:

    def __init__(self, use_cases: Iterable[UseCase]):
        self._use_cases: Dict[str, UseCase] = {uc.__class__.__name__: uc for uc in use_cases}

    def add(self, use_case: UseCase):
        self._use_cases[use_case.__class__.__name__] = use_case

    def update(self, use_cases: Iterable[UseCase]):
        self._use_cases.update({uc.__class__.__name__: uc for uc in use_cases})

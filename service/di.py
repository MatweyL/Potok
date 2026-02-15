from service.domain.use_cases.external.facade import UseCaseFacade

OBJECTS = {}


def set_use_case_facade(use_case_facade: UseCaseFacade):
    OBJECTS[UseCaseFacade.__name__] = use_case_facade


def get_use_case_facade() -> UseCaseFacade:
    return OBJECTS[UseCaseFacade.__name__]

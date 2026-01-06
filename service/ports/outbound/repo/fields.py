import enum
from typing import Optional, Any, List, Dict

from pydantic import BaseModel


class ConditionOperation(str, enum.Enum):
    GT = "GT"
    LT = "LT"
    GTE = "GTE"
    LTE = "LTE"
    EQ = "EQ"
    IS_NULL = "IS_NULL"
    NOT_NULL = "NOT_NULL"
    IN = "IN"


class LogicOperation(str, enum.Enum):
    AND = "AND"
    OR = "OR"


class BaseField(BaseModel):
    name: str
    value: Any


class UpdateField(BaseField):
    pass


class UpdateFields(BaseModel):
    group: List[UpdateField]

    def to_dict(self) -> Dict[str, Any]:
        return {update_field.name: update_field.value for update_field in self.group}

    @classmethod
    def single(cls, name: str, value: Any):
        return cls(group=[UpdateField(name=name, value=value)])

    @classmethod
    def multiple(cls, value_by_name: Dict[str, Any]):
        return cls(group=[UpdateField(name=name, value=value) for name, value in value_by_name.items()])


class FilterField(BaseField):
    operation: ConditionOperation = ConditionOperation.EQ


class FilterFieldsConjunct(BaseModel):
    group: List[FilterField]

    @classmethod
    def single(cls, name: str, value: Any, operation: ConditionOperation = ConditionOperation.EQ):
        return cls(group=[FilterField(name=name,
                                      value=value,
                                      operation=operation)])


class FilterFieldsDNF(BaseModel):
    conjunctions: List[FilterFieldsConjunct]

    @classmethod
    def single(cls, name: str, value: Any, operation: ConditionOperation = ConditionOperation.EQ):
        return cls(conjunctions=[FilterFieldsConjunct(group=[FilterField(name=name,
                                                                         value=value,
                                                                         operation=operation)])])

    @classmethod
    def single_conjunct(cls, filter_fields: List[FilterField]):
        return cls(conjunctions=[FilterFieldsConjunct(group=filter_fields)])


class PaginationQuery(BaseModel):
    offset_page: Optional[int] = None
    limit_per_page: Optional[int] = None
    order_by: Optional[str] = None
    asc_sort: Optional[bool] = None
    filter_fields_dnf: FilterFieldsDNF = None

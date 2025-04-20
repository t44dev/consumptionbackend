# stdlib
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

T = TypeVar("T")


class ApplyOperator(Enum):
    APPLY = 0
    ADD = 1
    SUB = 2


@dataclass
class ApplyQuery(Generic[T]):
    value: T
    operator: ApplyOperator


class WhereOperator(Enum):
    EQUAL = 0
    GT = 1
    GTE = 2
    LT = 3
    LTE = 4
    LIKE = 5


@dataclass
class WhereQuery(Generic[T]):
    value: T
    operator: ApplyOperator

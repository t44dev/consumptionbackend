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
    EQ = 0
    NEQ = 1
    GT = 2
    GTE = 3
    LT = 4
    LTE = 5
    LIKE = 6


@dataclass
class WhereQuery(Generic[T]):
    value: T
    operator: ApplyOperator

from dataclasses import dataclass
from enum import IntEnum


class ApplyOperator(IntEnum):
    APPLY = 0
    ADD = 1
    SUB = 2


@dataclass
class ApplyQuery[T]:
    value: T
    operator: ApplyOperator = ApplyOperator.APPLY


class WhereOperator(IntEnum):
    EQ = 0
    NEQ = 1
    GT = 2
    GTE = 3
    LT = 4
    LTE = 5
    LIKE = 6


@dataclass
class WhereQuery[T]:
    value: T
    operator: WhereOperator = WhereOperator.EQ

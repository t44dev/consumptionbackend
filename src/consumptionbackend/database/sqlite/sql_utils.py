# stdlib
from datetime import datetime
from enum import IntEnum
from functools import lru_cache
from typing import Any, TypeAlias

# consumption
from consumptionbackend.database.queries import (
    ApplyOperator,
    ApplyQuery,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import Id


SQLiteType: TypeAlias = None | Id | int | float | str


def to_sqlite_operator(
    column: str, query: ApplyQuery[Any] | WhereQuery[Any]
) -> tuple[str, SQLiteType]:
    value = fix_value(query.value)
    if isinstance(query, ApplyQuery):
        match query.operator:
            case ApplyOperator.APPLY:
                return f"{column} = ?", value
            case ApplyOperator.ADD:
                return f"{column} = {column} + ?", value
            case ApplyOperator.SUB:
                return f"{column} = {column} - ?", value

    else:  # WhereQuery
        match query.operator:
            case WhereOperator.EQ:
                return f"{column} {'IS' if value is None else '='} ?", value
            case WhereOperator.NEQ:
                return f"{column} != ?", value
            case WhereOperator.GT:
                return f"{column} > ?", value
            case WhereOperator.GTE:
                return f"{column} >= ?", value
            case WhereOperator.LT:
                return f"{column} < ?", value
            case WhereOperator.LTE:
                return f"{column} <= ?", value
            case WhereOperator.LIKE:
                assert isinstance(value, str)
                lower_value = f"%{str.lower(value)}%"
                return f"LOWER({column}) LIKE ?", lower_value


def fix_value(value: Any) -> SQLiteType:
    match value:
        case datetime():
            return value.timestamp()
        case IntEnum():
            return int(value)
        case _:
            return value


def validate_column_name(column: str) -> None:
    if any(map(lambda x: (ord(x) < 97 or ord(x) > 122) and ord(x) != 95, column)):
        raise RuntimeError(
            "Invalid column name on insert. Column names must only include a-z and _."
        )


@lru_cache(maxsize=10)
def to_shorthand(table_name: str) -> str:
    return "".join(map(lambda x: x[0], table_name.split("_")))


def placeholders(count: int, placeholder: str = "?") -> str:
    placeholders = ", ".join([placeholder for _ in range(count)])
    return placeholders

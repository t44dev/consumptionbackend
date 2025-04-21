# stdlib
from datetime import datetime
from typing import NotRequired, TypedDict

# consumption
from .queries import ApplyQuery, WhereQuery
from consumptionbackend.entities import Status

# Base


class BaseFieldsRequired(TypedDict):
    id: int


class BaseWhereMapping(TypedDict, total=False):
    id: list[WhereQuery[int]]


# Consumables


class ConsumableFieldsRequired(BaseFieldsRequired):
    series_id: NotRequired[int]
    name: str
    type: str
    status: NotRequired[Status]
    parts: NotRequired[int]
    max_parts: NotRequired[int | None]
    completions: NotRequired[int]
    rating: NotRequired[float | None]
    start_date: NotRequired[datetime]
    end_date: NotRequired[datetime]


class ConsumableApplyMapping(TypedDict, total=False):
    series_id: ApplyQuery[int]
    name: ApplyQuery[str]
    type: ApplyQuery[str]
    status: ApplyQuery[Status]
    parts: ApplyQuery[int]
    max_parts: ApplyQuery[int | None]
    completions: ApplyQuery[int]
    rating: ApplyQuery[float | None]
    start_date: ApplyQuery[datetime]
    end_date: ApplyQuery[datetime]


class ConsumableWhereMapping(BaseWhereMapping, total=False):
    series_id: list[WhereQuery[int]]
    name: list[WhereQuery[str]]
    type: list[WhereQuery[str]]
    status: list[WhereQuery[Status]]
    parts: list[WhereQuery[int]]
    max_parts: list[WhereQuery[int | None]]
    completions: list[WhereQuery[int]]
    rating: list[WhereQuery[float | None]]
    start_date: list[WhereQuery[datetime]]
    end_date: list[WhereQuery[datetime]]


# Series


class SeriesFieldsRequired(BaseFieldsRequired):
    name: str


class SeriesApplyMapping(TypedDict, total=False):
    name: ApplyQuery[str]


class SeriesWhereMapping(BaseWhereMapping, total=False):
    name: list[WhereQuery[str]]


# Personnel


class PersonnelFieldsRequired(BaseFieldsRequired):
    first_name: NotRequired[str]
    last_name: NotRequired[str]
    pseudonym: NotRequired[str]


class PersonnelApplyMapping(TypedDict, total=False):
    first_name: ApplyQuery[str]
    last_name: ApplyQuery[str]
    pseudonym: ApplyQuery[str]


class PersonnelWhereMapping(BaseWhereMapping, total=False):
    first_name: list[WhereQuery[str]]
    last_name: list[WhereQuery[str]]
    pseudonym: list[WhereQuery[str]]
    role: list[WhereQuery[str]]


# Tag


class TagWhereMapping(TypedDict, total=False):
    tag: list[WhereQuery[str]]

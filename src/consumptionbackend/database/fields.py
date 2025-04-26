# stdlib
from collections.abc import Sequence
from datetime import datetime
from typing import NotRequired, TypedDict

# consumption
from .queries import ApplyQuery, WhereQuery
from consumptionbackend.entities import Status

# Base


class BaseFieldsRequired(TypedDict):
    pass


class BaseWhereMapping(TypedDict, total=False):
    id: Sequence[WhereQuery[int]]


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
    start_date: NotRequired[datetime | None]
    end_date: NotRequired[datetime | None]


class ConsumableApplyMapping(TypedDict, total=False):
    series_id: ApplyQuery[int]
    name: ApplyQuery[str]
    type: ApplyQuery[str]
    status: ApplyQuery[Status]
    parts: ApplyQuery[int]
    max_parts: ApplyQuery[int | None]
    completions: ApplyQuery[int]
    rating: ApplyQuery[float | None]
    start_date: ApplyQuery[datetime | None]
    end_date: ApplyQuery[datetime | None]


class ConsumableWhereMapping(BaseWhereMapping, total=False):
    series_id: Sequence[WhereQuery[int]]
    name: Sequence[WhereQuery[str]]
    type: Sequence[WhereQuery[str]]
    status: Sequence[WhereQuery[Status]]
    parts: Sequence[WhereQuery[int]]
    max_parts: Sequence[WhereQuery[int | None]]
    completions: Sequence[WhereQuery[int]]
    rating: Sequence[WhereQuery[float | None]]
    start_date: Sequence[WhereQuery[datetime | None]]
    end_date: Sequence[WhereQuery[datetime | None]]


# Series


class SeriesFieldsRequired(BaseFieldsRequired):
    name: str


class SeriesApplyMapping(TypedDict, total=False):
    name: ApplyQuery[str]


class SeriesWhereMapping(BaseWhereMapping, total=False):
    name: Sequence[WhereQuery[str]]


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
    first_name: Sequence[WhereQuery[str]]
    last_name: Sequence[WhereQuery[str]]
    pseudonym: Sequence[WhereQuery[str]]
    role: Sequence[WhereQuery[str]]


# Tag


class TagWhereMapping(TypedDict, total=False):
    tag: Sequence[WhereQuery[str]]

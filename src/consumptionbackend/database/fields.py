from collections.abc import Sequence
from datetime import datetime
from typing import NotRequired, TypedDict, cast

from consumptionbackend.entities import Id, Status

from .queries import ApplyQuery, WhereQuery

# Base


class WhereMapping(TypedDict, total=False):
    consumables: ConsumableWhereMapping
    series: SeriesWhereMapping
    personnel: PersonnelWhereMapping


class BaseFieldsRequired(TypedDict): ...


class BaseWhereMapping(TypedDict, total=False):
    id: Sequence[WhereQuery[Id]]


# Consumables


class ConsumableFieldsRequired(BaseFieldsRequired):
    series_id: NotRequired[Id]
    name: str
    type: str
    status: NotRequired[Status]
    parts: NotRequired[int]
    max_parts: NotRequired[int | None]
    completions: NotRequired[int]
    rating: NotRequired[float | None]
    start_date: NotRequired[datetime | None]
    end_date: NotRequired[datetime | None]
    tags: NotRequired[Sequence[str]]


class ConsumableApplyMapping(TypedDict, total=False):
    series_id: ApplyQuery[Id]
    name: ApplyQuery[str]
    type: ApplyQuery[str]
    status: ApplyQuery[Status]
    parts: ApplyQuery[int]
    max_parts: ApplyQuery[int | None]
    completions: ApplyQuery[int]
    rating: ApplyQuery[float | None]
    start_date: ApplyQuery[datetime | None]
    end_date: ApplyQuery[datetime | None]
    tags: Sequence[ApplyQuery[str]]


class ConsumableWhereMapping(BaseWhereMapping, total=False):
    series_id: Sequence[WhereQuery[Id]]
    name: Sequence[WhereQuery[str]]
    type: Sequence[WhereQuery[str]]
    status: Sequence[WhereQuery[Status]]
    parts: Sequence[WhereQuery[int]]
    max_parts: Sequence[WhereQuery[int | None]]
    completions: Sequence[WhereQuery[int]]
    rating: Sequence[WhereQuery[float | None]]
    start_date: Sequence[WhereQuery[datetime | None]]
    end_date: Sequence[WhereQuery[datetime | None]]
    tags: Sequence[WhereQuery[str]]


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
    first_name: ApplyQuery[str | None]
    last_name: ApplyQuery[str | None]
    pseudonym: ApplyQuery[str | None]


class PersonnelWhereMapping(BaseWhereMapping, total=False):
    first_name: Sequence[WhereQuery[str]]
    last_name: Sequence[WhereQuery[str]]
    pseudonym: Sequence[WhereQuery[str]]
    role: Sequence[WhereQuery[str]]


# Conversions


def consumable_required_to_where(required: ConsumableFieldsRequired) -> WhereMapping:
    return {
        "consumables": cast(
            ConsumableWhereMapping,
            cast(object, {k: [WhereQuery(v)] for k, v in required.items()}),
        )
    }


def series_required_to_where(required: SeriesFieldsRequired) -> WhereMapping:
    return {
        "series": cast(
            SeriesWhereMapping,
            cast(object, {k: [WhereQuery(v)] for k, v in required.items()}),
        )
    }


def personnel_required_to_where(required: PersonnelFieldsRequired) -> WhereMapping:
    return {
        "personnel": cast(
            PersonnelWhereMapping,
            cast(object, {k: [WhereQuery(v)] for k, v in required.items()}),
        )
    }

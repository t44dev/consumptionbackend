from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, TypedDict, Unpack

from consumptionbackend.entities import EntityBase, Id

from .fields import (
    ConsumableWhereMapping,
    PersonnelWhereMapping,
    SeriesWhereMapping,
)


class WhereMapping(TypedDict, total=False):
    consumables: ConsumableWhereMapping
    series: SeriesWhereMapping
    personnel: PersonnelWhereMapping


class DatabaseHandlerBase[E: EntityBase](ABC):
    @classmethod
    @abstractmethod
    def new(cls, **values: Any) -> Id:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: Id) -> E:
        pass

    @classmethod
    @abstractmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[E]:
        pass

    @classmethod
    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[E]:
        pass

    @classmethod
    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: Any,
    ) -> Sequence[Id]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int:
        pass

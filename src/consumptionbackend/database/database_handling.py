# stdlib
from abc import abstractmethod, ABC
from typing import Any, Generic, TypeVar, TypedDict, Unpack
from collections.abc import Sequence

# consumption
from .fields import (
    ConsumableWhereMapping,
    PersonnelWhereMapping,
    SeriesWhereMapping,
    TagWhereMapping,
)
from consumptionbackend.entities import EntityBase


E = TypeVar("E", bound=EntityBase)


class WhereMapping(TypedDict, total=False):
    consumables: ConsumableWhereMapping
    series: SeriesWhereMapping
    personnel: PersonnelWhereMapping
    consumable_tags: TagWhereMapping


class DatabaseHandlerBase(Generic[E], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Any) -> E:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: int) -> E:
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
    ) -> Sequence[E]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        pass

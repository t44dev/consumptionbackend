# stdlib
from abc import abstractmethod, ABC
from typing import Any, Generic, TypeVar, TypedDict, Unpack
from collections.abc import Sequence

# consumption
from .fields import (
    ConsumableWhereMapping,
    PersonnelWhereMapping,
    SeriesWhereMapping,
)
from consumptionbackend.entities import EntityBase, Id


E = TypeVar("E", bound=EntityBase)


class WhereMapping(TypedDict, total=False):
    consumables: ConsumableWhereMapping
    series: SeriesWhereMapping
    personnel: PersonnelWhereMapping


class DatabaseHandlerBase(Generic[E], ABC):

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

# stdlib
from abc import abstractmethod, ABC
from typing import Any, TypeAlias, TypeVar, TypedDict, Unpack
from collections.abc import Sequence, Mapping


# consumption
from .fields import (
    ConsumableWhereMapping,
    PersonnelWhereMapping,
    SeriesWhereMapping,
    TagWhereMapping,
)
from .queries import ApplyQuery
from consumptionbackend.utils import AbstractSingleton
from consumptionbackend.entities import EntityBase


E = TypeVar("E", bound=EntityBase)


# TODO: These keys are the same as the SQLite table names... too coupled?
class WhereMapping(TypedDict, total=False):
    consumables: ConsumableWhereMapping
    series: SeriesWhereMapping
    personnel: PersonnelWhereMapping
    consumable_tags: TagWhereMapping


ApplyMapping: TypeAlias = Mapping[str, ApplyQuery[Any]]


class DatabaseHandlerBase(AbstractSingleton, ABC):

    @abstractmethod
    def new(self, t: type[E], **values: Any) -> E:
        pass

    @abstractmethod
    def find_by_id(self, t: type[E], id: int) -> E:
        pass

    @abstractmethod
    def find(self, t: type[E], **where: Unpack[WhereMapping]) -> Sequence[E]:
        pass

    @abstractmethod
    def update(
        self,
        t: type[E],
        where: WhereMapping,
        apply: ApplyMapping,
    ) -> Sequence[E]:
        pass

    @abstractmethod
    def delete(self, t: type[E], **where: Unpack[WhereMapping]) -> None:
        pass


class EntityHandlerBase(ABC):
    def __init__(self) -> None:
        raise RuntimeError(
            f"Attempted to instantiate DatabaseHandler {type(self).__name__}. DatabaseHandler cannot be used outside of a static context."
        )

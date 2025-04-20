# stdlib
from abc import abstractmethod, ABC
from typing import Any, TypeVar, TypedDict, Unpack
from collections.abc import Sequence, Mapping

# consumption
from consumptionbackend.database.fields import ConsumableWhereMapping
from consumptionbackend.database.queries import ApplyQuery
from consumptionbackend.entities import EntityBase
from consumptionbackend.utils import Singleton

E = TypeVar("E", bound=EntityBase)
V = TypeVar("V")


class WhereMapping(TypedDict, total=False):
    consumable: ConsumableWhereMapping
    series: ConsumableWhereMapping
    personnel: ConsumableWhereMapping


class DatabaseHandlerBase(ABC, Singleton):

    @abstractmethod
    def new(self, t: type[E], **values: Mapping[str, Any]) -> E:
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
        apply: Mapping[str, ApplyQuery[V]],
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

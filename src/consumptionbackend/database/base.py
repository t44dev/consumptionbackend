from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Unpack

from consumptionbackend.database.fields import WhereMapping
from consumptionbackend.entities import EntityBase, Id


class EntityServiceBase[E: EntityBase](ABC):
    @abstractmethod
    def new(cls, **values: Any) -> Id: ...

    @abstractmethod
    def find_by_id(cls, id: Id) -> E: ...

    @abstractmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[E]: ...

    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[E]: ...

    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: Any,
    ) -> Sequence[Id]: ...

    @abstractmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int: ...

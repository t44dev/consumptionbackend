from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Unpack

from consumptionbackend.database.fields import WhereMapping
from consumptionbackend.entities import EntityBase, Id


class DatabaseHandlerBase[E: EntityBase](ABC):
    @classmethod
    @abstractmethod
    def new(cls, **values: Any) -> Id: ...

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: Id) -> E: ...

    @classmethod
    @abstractmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[E]: ...

    @classmethod
    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[E]: ...

    @classmethod
    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: Any,
    ) -> Sequence[Id]: ...

    @classmethod
    @abstractmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int: ...

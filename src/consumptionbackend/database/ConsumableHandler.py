from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import (
    Consumable,
    Id,
    IdRoles,
    Series,
)

from .database_handling import DatabaseHandlerBase, WhereMapping
from .fields import ConsumableFieldsRequired
from .queries import ApplyQuery


class ConsumableHandlerBase(DatabaseHandlerBase[Consumable], ABC):
    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Id: ...

    @classmethod
    @abstractmethod
    def series(cls, id: Id) -> Series: ...

    @classmethod
    @abstractmethod
    def personnel(cls, consumable_id: Id) -> Sequence[IdRoles]: ...

    @classmethod
    @abstractmethod
    def tags(cls, consumable_id: Id) -> Sequence[str]: ...

    @classmethod
    @abstractmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[Id]: ...

# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

# consumption
from .database_handling import DatabaseHandlerBase, WhereMapping
from .queries import ApplyQuery
from .fields import ConsumableFieldsRequired
from consumptionbackend.entities import (
    Consumable,
    EntityRoles,
    Id,
    Series,
)


class ConsumableHandlerBase(DatabaseHandlerBase[Consumable], ABC):

    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Id:
        pass

    @classmethod
    @abstractmethod
    def series(cls, id: Id) -> Series:
        pass

    @classmethod
    @abstractmethod
    def personnel_by_id(cls, consumable_id: Id) -> Sequence[EntityRoles]:
        pass

    @classmethod
    @abstractmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[Id]:
        pass

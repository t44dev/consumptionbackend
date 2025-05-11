# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase, WhereMapping
from .queries import ApplyQuery
from .fields import ConsumableFieldsRequired
from consumptionbackend.entities import (
    Consumable,
    Series,
    PersonnelRoles,
    ConsumablePersonnel,
)


class ConsumableHandlerBase(DatabaseHandlerBase[Consumable], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        pass

    @classmethod
    @abstractmethod
    def series(cls, id: int) -> Series:
        pass

    @classmethod
    @abstractmethod
    def personnel_by_id(cls, consumable_id: int) -> Sequence[PersonnelRoles]:
        pass

    @classmethod
    @abstractmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[ConsumablePersonnel]:
        pass

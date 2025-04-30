# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import ConsumableFieldsRequired
from consumptionbackend.entities import Consumable, Series, PersonnelRoles


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
    def personnel(cls, id: int) -> Sequence[PersonnelRoles]:
        pass

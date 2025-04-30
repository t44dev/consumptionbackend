# stdlib
from abc import ABC, abstractmethod
from typing import Unpack
from collections.abc import Sequence

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import PersonnelFieldsRequired
from consumptionbackend.entities import Personnel, ConsumableRoles


class PersonnelHandlerBase(DatabaseHandlerBase[Personnel], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        pass

    @classmethod
    @abstractmethod
    def consumables(cls, id: int) -> Sequence[ConsumableRoles]:
        pass

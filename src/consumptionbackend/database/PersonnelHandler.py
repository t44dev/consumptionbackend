# stdlib
from abc import ABC, abstractmethod
from typing import Unpack, override
from collections.abc import Sequence

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import PersonnelFieldsRequired
from consumptionbackend.entities import EntityRoles, Id, Personnel


class PersonnelHandlerBase(DatabaseHandlerBase[Personnel], ABC):

    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Id:
        pass

    @classmethod
    @abstractmethod
    def consumables(cls, personnel_id: Id) -> Sequence[EntityRoles]:
        pass

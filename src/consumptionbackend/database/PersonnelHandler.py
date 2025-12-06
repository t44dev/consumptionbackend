from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import Id, IdRoles, Personnel

from .database_handling import DatabaseHandlerBase
from .fields import PersonnelFieldsRequired


class PersonnelHandlerBase(DatabaseHandlerBase[Personnel], ABC):
    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Id:
        pass

    @classmethod
    @abstractmethod
    def consumables(cls, personnel_id: Id) -> Sequence[IdRoles]:
        pass

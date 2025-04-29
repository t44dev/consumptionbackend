# stdlib
from abc import ABC, abstractmethod
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import PersonnelFieldsRequired
from consumptionbackend.entities import Personnel


class PersonnelHandlerBase(DatabaseHandlerBase[Personnel], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        pass

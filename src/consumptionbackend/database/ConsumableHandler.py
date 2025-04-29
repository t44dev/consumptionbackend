# stdlib
from abc import ABC, abstractmethod
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import ConsumableFieldsRequired
from consumptionbackend.entities import Consumable


class ConsumableHandlerBase(DatabaseHandlerBase[Consumable], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        pass

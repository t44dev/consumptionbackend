# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import SeriesFieldsRequired
from consumptionbackend.entities import Consumable, Series


class SeriesHandlerBase(DatabaseHandlerBase[Series], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        pass

    @classmethod
    @abstractmethod
    def consumables(cls, id: int) -> Sequence[Consumable]:
        pass

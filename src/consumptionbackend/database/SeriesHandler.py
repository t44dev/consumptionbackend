# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import SeriesFieldsRequired
from consumptionbackend.entities import Consumable, Id, Series


class SeriesHandlerBase(DatabaseHandlerBase[Series], ABC):

    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Id:
        pass

    @classmethod
    @abstractmethod
    def consumables(cls, id: Id) -> Sequence[Consumable]:
        pass

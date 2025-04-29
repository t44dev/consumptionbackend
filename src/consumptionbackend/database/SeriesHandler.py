# stdlib
from abc import ABC, abstractmethod
from typing import Unpack

# consumption
from .database_handling import DatabaseHandlerBase
from .fields import SeriesFieldsRequired
from consumptionbackend.entities import Series


class SeriesHandlerBase(DatabaseHandlerBase[Series], ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        pass

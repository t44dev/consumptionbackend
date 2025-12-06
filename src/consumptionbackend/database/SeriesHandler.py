from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import Consumable, Id, Series

from .database_handling import DatabaseHandlerBase
from .fields import SeriesFieldsRequired


class SeriesHandlerBase(DatabaseHandlerBase[Series], ABC):
    @override
    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Id: ...

    @classmethod
    @abstractmethod
    def consumables(cls, id: Id) -> Sequence[Consumable]: ...

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import Consumable, Id, Series
from consumptionbackend.utils import ServiceBase

from .base import EntityServiceBase
from .fields import SeriesFieldsRequired


class SeriesService(EntityServiceBase[Series], ServiceBase, ABC):
    @override
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Id: ...

    @abstractmethod
    def consumables(cls, series_id: Id) -> Sequence[Consumable]: ...

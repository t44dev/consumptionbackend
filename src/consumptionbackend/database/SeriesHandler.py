# stdlib

# consumption
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack
from .database_handling import EntityHandlerBase, WhereMapping
from .fields import (
    SeriesApplyMapping,
    SeriesFieldsRequired,
)
from consumptionbackend.entities import Series


class SeriesHandlerBase(EntityHandlerBase, ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: int) -> Series:
        pass

    @classmethod
    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        pass

    @classmethod
    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[int]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

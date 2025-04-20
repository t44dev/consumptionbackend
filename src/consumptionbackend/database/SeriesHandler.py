# stdlib

# consumption
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack
from consumptionbackend.database.base_handlers import EntityHandlerBase, WhereMapping
from consumptionbackend.database.fields import (
    SeriesApplyMapping,
    SeriesFieldsRequired,
)
from consumptionbackend.entities import Series


class SeriesHandlerBase(EntityHandlerBase, ABC):

    @abstractmethod
    @classmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        pass

    @abstractmethod
    @classmethod
    def find_by_id(cls, id: int) -> Series:
        pass

    @abstractmethod
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        pass

    @abstractmethod
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[int]:
        pass

    @abstractmethod
    @classmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

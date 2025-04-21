# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack

# consumption
from .database_handling import EntityHandlerBase, WhereMapping
from .fields import (
    ConsumableApplyMapping,
    ConsumableFieldsRequired,
)
from consumptionbackend.entities import Consumable


class ConsumableHandlerBase(EntityHandlerBase, ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: int) -> Consumable:
        pass

    @classmethod
    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        pass

    @classmethod
    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[int]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

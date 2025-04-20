# stdlib

# consumption
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack
from consumptionbackend.database.base_handlers import EntityHandlerBase, WhereMapping
from consumptionbackend.database.fields import (
    ConsumableApplyMapping,
    ConsumableFieldsRequired,
)
from consumptionbackend.entities import Consumable


class ConsumableHandlerBase(EntityHandlerBase, ABC):

    @abstractmethod
    @classmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        pass

    @abstractmethod
    @classmethod
    def find_by_id(cls, id: int) -> Consumable:
        pass

    @abstractmethod
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        pass

    @abstractmethod
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[int]:
        pass

    @abstractmethod
    @classmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

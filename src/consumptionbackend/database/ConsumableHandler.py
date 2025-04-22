# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, cast

# consumption
from .database_handling import (
    ApplyMapping,
    DatabaseHandlerBase,
    EntityHandlerBase,
    WhereMapping,
)
from .fields import (
    ConsumableApplyMapping,
    ConsumableFieldsRequired,
)
from consumptionbackend.entities import Consumable


class ConsumableHandlerBase(EntityHandlerBase, ABC):

    HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        return cls.HANDLER().new(Consumable, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Consumable:
        return cls.HANDLER().find_by_id(Consumable, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        return cls.HANDLER().find(Consumable, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[Consumable]:
        return cls.HANDLER().update(Consumable, where, cast(ApplyMapping, apply))

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls.HANDLER().delete(Consumable, **where)

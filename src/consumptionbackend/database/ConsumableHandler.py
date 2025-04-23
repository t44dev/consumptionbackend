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
    ConsumableWhereMapping,
)
from consumptionbackend.entities import Consumable


class ConsumableHandlerBase(EntityHandlerBase, ABC):

    _HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        return cls._HANDLER().new(Consumable, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Consumable:
        return cls._HANDLER().find_by_id(Consumable, id)

    @classmethod
    def find(cls, **where: Unpack[ConsumableWhereMapping]) -> Sequence[Consumable]:
        return cls._HANDLER().find(Consumable, **where)

    @classmethod
    def update(
        cls,
        where: ConsumableWhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[Consumable]:
        return cls._HANDLER().update(
            Consumable,
            cast(WhereMapping, cast(object, where)),
            cast(ApplyMapping, apply),
        )

    @classmethod
    def delete(cls, **where: Unpack[ConsumableWhereMapping]) -> None:
        return cls._HANDLER().delete(Consumable, **where)

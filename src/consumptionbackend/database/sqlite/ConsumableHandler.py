# stdlib
from typing import Unpack, final
from collections.abc import Sequence

# consumption
from consumptionbackend.entities import Consumable
from consumptionbackend.database import (
    ConsumableHandlerBase,
    ConsumableFieldsRequired,
    WhereMapping,
    ApplyMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler


@final
class SQLiteConsumableHandlerBase(ConsumableHandlerBase):

    _HANDLER = SQLiteDatabaseHandler

    @classmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Consumable:
        return cls._HANDLER.new(Consumable, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Consumable:
        return cls._HANDLER.find_by_id(Consumable, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        return cls._HANDLER.find(Consumable, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: ApplyMapping,
    ) -> Sequence[Consumable]:
        return cls._HANDLER.update(Consumable, where, apply)

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls._HANDLER.delete(Consumable, **where)

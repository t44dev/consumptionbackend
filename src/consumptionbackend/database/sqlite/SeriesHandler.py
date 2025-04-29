# stdlib
from typing import Unpack, final
from collections.abc import Sequence

# consumption
from consumptionbackend.database.fields import SeriesApplyMapping
from consumptionbackend.entities import Series
from consumptionbackend.database import (
    SeriesHandlerBase,
    SeriesFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler


@final
class SQLiteSeriesHandlerBase(SeriesHandlerBase):

    _HANDLER = SQLiteDatabaseHandler

    @classmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        return cls._HANDLER.new(Series, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Series:
        return cls._HANDLER.find_by_id(Series, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        return cls._HANDLER.find(Series, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[Series]:
        return cls._HANDLER.update(Series, where, apply)

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls._HANDLER.delete(Series, **where)

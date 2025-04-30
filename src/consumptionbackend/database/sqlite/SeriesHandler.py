# stdlib
import sqlite3
from typing import Unpack, final
from collections.abc import Sequence

# consumption
from consumptionbackend.database.fields import SeriesApplyMapping
from consumptionbackend.entities import Consumable, Series
from consumptionbackend.database import (
    SeriesHandlerBase,
    SeriesFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .sql_utils import SQLiteType


@final
class SQLiteSeriesHandler(SeriesHandlerBase):

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

    @classmethod
    def consumables(cls, id: int) -> Sequence[Consumable]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._consumables_sql(id))
        ).fetchall()

        cur.close()

        return [Consumable(**args) for args in results]

    @classmethod
    def _consumables_sql(cls, id: int) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT * 
            FROM {cls._HANDLER.TABLE_MAPPING[Consumable]}
            WHERE series_id = ?
        """

        return sql, [id]

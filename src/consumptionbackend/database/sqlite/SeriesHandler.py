import sqlite3
from collections.abc import Sequence
from typing import Unpack, final, override

from consumptionbackend.database import (
    SeriesFieldsRequired,
    SeriesHandlerBase,
    WhereMapping,
)
from consumptionbackend.database.fields import SeriesApplyMapping
from consumptionbackend.entities import Consumable, Id, Series

from .sql_utils import SQLiteType
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler


@final
class SQLiteSeriesHandler(SeriesHandlerBase):
    _HANDLER = SQLiteDatabaseHandler

    @override
    @classmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Id:
        return cls._HANDLER.new(Series, **values)

    @override
    @classmethod
    def find_by_id(cls, id: Id) -> Series:
        return cls._HANDLER.find_by_id(Series, id)

    @override
    @classmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[Series]:
        return cls._HANDLER.find_by_ids(Series, ids)

    @override
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        return cls._HANDLER.find(Series, **where)

    @override
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[Id]:
        return cls._HANDLER.update(Series, where, apply)

    @override
    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int:
        return cls._HANDLER.delete(Series, **where)

    @override
    @classmethod
    def consumables(cls, id: Id) -> Sequence[Consumable]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._consumables_sql(id))
        ).fetchall()

        cur.close()

        return [Consumable(**args) for args in results]

    @classmethod
    def _consumables_sql(cls, id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT * 
            FROM {cls._HANDLER.TABLE_MAPPING[Consumable]}
            WHERE series_id = ?
        """

        return sql, [id]

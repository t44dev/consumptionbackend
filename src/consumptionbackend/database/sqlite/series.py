import logging
import sqlite3
from collections.abc import Sequence
from typing import Unpack, final, override

from consumptionbackend.database import (
    SeriesFieldsRequired,
    SeriesService,
    WhereMapping,
)
from consumptionbackend.database.fields import SeriesApplyMapping
from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.entities import Consumable, Id, Series
from consumptionbackend.utils import ServiceProvider

from .helper import SQLiteDatabaseHelper
from .sql_utils import SQLiteType

logger = logging.getLogger(__name__)


@final
class SQLiteSeriesService(SeriesService):
    @override
    def new(self, **values: Unpack[SeriesFieldsRequired]) -> Id:
        return SQLiteDatabaseHelper.new(Series, **values)

    @override
    def find_by_id(self, id: Id) -> Series:
        return SQLiteDatabaseHelper.find_by_id(Series, id)

    @override
    def find_by_ids(self, ids: Sequence[Id]) -> Sequence[Series]:
        return SQLiteDatabaseHelper.find_by_ids(Series, ids)

    @override
    def find(self, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        return SQLiteDatabaseHelper.find(Series, **where)

    @override
    def update(
        self,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[Id]:
        return SQLiteDatabaseHelper.update(Series, where, apply)

    @override
    def delete(self, **where: Unpack[WhereMapping]) -> int:
        return SQLiteDatabaseHelper.delete(Series, **where)

    @override
    def consumables(self, series_id: Id) -> Sequence[Consumable]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(self._consumables_sql(series_id))
        ).fetchall()

        cur.close()

        consumables = [Consumable(**args) for args in results]
        logger.info(
            "Found Consumables for Series id",
            extra={
                "data": {"id": series_id, "results": consumables},
            },
        )

        return consumables

    def _consumables_sql(self, id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT * 
            FROM {SQLiteDatabaseHelper.TABLE_MAPPING[Consumable]}
            WHERE series_id = ?
        """

        return sql, [id]

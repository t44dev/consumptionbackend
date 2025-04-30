# stdlib
from collections import defaultdict
import sqlite3
from typing import Unpack, final
from collections.abc import MutableMapping, MutableSequence, Sequence

# consumption
from consumptionbackend.database.fields import ConsumableApplyMapping
from consumptionbackend.entities import Consumable, Series, PersonnelRoles
from consumptionbackend.database import (
    ConsumableHandlerBase,
    ConsumableFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .sql_utils import SQLiteType
from .PersonnelHandler import SQLitePersonnelHandler


@final
class SQLiteConsumableHandler(ConsumableHandlerBase):

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
        apply: ConsumableApplyMapping,
    ) -> Sequence[Consumable]:
        return cls._HANDLER.update(Consumable, where, apply)

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls._HANDLER.delete(Consumable, **where)

    @classmethod
    def series(cls, id: int) -> Series:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        result: sqlite3.Row = cur.execute(*(cls._series_sql(id))).fetchone()

        cur.close()
        return Series(**result)

    @classmethod
    def _series_sql(cls, id: int) -> tuple[str, list[SQLiteType]]:
        sql = f"""
        SELECT * 
            FROM {cls._HANDLER.TABLE_MAPPING[Series]} t1
            WHERE t1.id = (
                SELECT t2.series_id 
                    FROM {cls._HANDLER.TABLE_MAPPING[Consumable]} t2
                    WHERE t2.id = ? 
            )
        """

        return sql, [id]

    @classmethod
    def personnel(cls, id: int) -> Sequence[PersonnelRoles]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._personnel_sql(id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[int, MutableSequence[str]] = defaultdict(list)
        for row in results:
            p_id = row["id"]
            role = row["role"]
            mapping[p_id].append(role)
        return [
            PersonnelRoles(SQLitePersonnelHandler.find_by_id(p_id), mapping[p_id])
            for p_id in mapping
        ]

    @classmethod
    def _personnel_sql(cls, id: int) -> tuple[str, list[SQLiteType]]:
        sql = f"""
        SELECT personnel_id as id, role
            FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id = ?
        """

        return sql, [id]

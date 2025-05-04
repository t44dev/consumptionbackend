# stdlib
from collections import defaultdict
import sqlite3
from typing import Unpack, final
from collections.abc import MutableMapping, MutableSequence, Sequence

# consumption
from consumptionbackend.database.fields import ConsumableApplyMapping
from consumptionbackend.entities import (
    Consumable,
    Series,
    PersonnelRoles,
    ConsumablePersonnel,
)
from consumptionbackend.database import (
    ConsumableHandlerBase,
    ConsumableFieldsRequired,
    WhereMapping,
)
from consumptionbackend.entities.Personnel import Personnel
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .sql_utils import SQLiteType, to_shorthand


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
    def personnel_by_id(cls, consumable_id: int) -> Sequence[PersonnelRoles]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._personnel_sql(consumable_id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[int, MutableSequence[str]] = defaultdict(list)
        for row in results:
            p_id = row["id"]
            role = row["role"]
            mapping[p_id].append(role)
        return [
            PersonnelRoles(ph.SQLitePersonnelHandler.find_by_id(p_id), mapping[p_id])
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

    @classmethod
    def add_personnel(
        cls, consumable_where: WhereMapping, personnel_where: WhereMapping, role: str
    ) -> Sequence[ConsumablePersonnel]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        result: Sequence[sqlite3.Row] = cur.execute(
            *(cls._add_personnel_sql(consumable_where, personnel_where, role))
        ).fetchall()

        cls._HANDLER.PROVIDER().db.commit()
        cur.close()
        return [
            ConsumablePersonnel(
                cls.find_by_id(row["consumable_id"]),
                cls.personnel_by_id(row["consumable_id"]),
            )
            for row in result
        ]

    @classmethod
    def _add_personnel_sql(
        cls, consumable_where: WhereMapping, personnel_where: WhereMapping, role: str
    ) -> tuple[str, Sequence[SQLiteType]]:
        consumable_where_query, consumable_values = cls._HANDLER.where_query(
            consumable_where
        )
        personnel_where_query, personnel_values = cls._HANDLER.where_query(
            personnel_where
        )

        sql = f"""
        INSERT INTO {cls._HANDLER.PERSONNEL_MAPPING_TABLE} (consumable_id, personnel_id, role)
            VALUES (
                (
                    SELECT {to_shorthand(cls._HANDLER.TABLE_MAPPING[Consumable])}.id as consumable_id
                    FROM {cls._HANDLER.MEGATABLE_QUERY}
                    {consumable_where_query}
                )
                CROSS JOIN
                (
                    SELECT {to_shorthand(cls._HANDLER.TABLE_MAPPING[Personnel])}.id as personnel_id
                    FROM {cls._HANDLER.MEGATABLE_QUERY}
                    {personnel_where_query}
                )
                CROSS JOIN
                (
                    SELECT ? as role
                )
            )
        RETURNING *
        """

        return sql, consumable_values + personnel_values + [role]

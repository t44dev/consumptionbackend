# stdlib
from collections import defaultdict
import sqlite3
from typing import Unpack, final
from collections.abc import MutableMapping, MutableSequence, MutableSet, Sequence, Set

# consumption
import consumptionbackend.database.sqlite.PersonnelHandler as ph
from consumptionbackend.database.queries import ApplyOperator, ApplyQuery
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
            *(cls._personnel_by_id_sql(consumable_id))
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
    def _personnel_by_id_sql(cls, id: int) -> tuple[str, list[SQLiteType]]:
        sql = f"""
        SELECT personnel_id as id, role
            FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id = ?
        """

        return sql, [id]

    @classmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[ConsumablePersonnel]:
        add_roles: MutableSet[str] = set()
        remove_roles: MutableSet[str] = set()

        for role_query in roles:
            match role_query.operator:
                case ApplyOperator.APPLY | ApplyOperator.ADD:
                    add_roles.add(role_query.value)
                    if role_query.value in remove_roles:
                        remove_roles.remove(role_query.value)

                case ApplyOperator.SUB:
                    remove_roles.add(role_query.value)
                    if role_query.value in add_roles:
                        add_roles.remove(role_query.value)

        if len(add_roles) > 0:
            cls._add_personnel(consumable_where, personnel_where, add_roles)
        if len(remove_roles) > 0:
            cls._remove_personnel(consumable_where, personnel_where, remove_roles)

        consumables = cls.find(**consumable_where)
        return [
            ConsumablePersonnel(consumable, cls.personnel_by_id(consumable.id))
            for consumable in consumables
        ]

    @classmethod
    def _add_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> None:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        for role in roles:
            _ = cur.execute(
                *(cls._add_personnel_sql(consumable_where, personnel_where, role))
            )

        cls._HANDLER.PROVIDER().db.commit()
        cur.close()

    @classmethod
    def _add_personnel_sql(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        role: str,
    ) -> tuple[str, Sequence[SQLiteType]]:
        consumable_where_query, consumable_values = cls._HANDLER.where_query(
            consumable_where
        )
        personnel_where_query, personnel_values = cls._HANDLER.where_query(
            personnel_where
        )

        sql = f"""
        INSERT OR IGNORE INTO {cls._HANDLER.PERSONNEL_MAPPING_TABLE} (consumable_id, personnel_id, role)
            SELECT * FROM 
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
        """

        return sql, consumable_values + personnel_values + [role]

    @classmethod
    def _remove_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> None:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        _ = cur.execute(
            *(cls._remove_personnel_sql(consumable_where, personnel_where, roles))
        )

        cls._HANDLER.PROVIDER().db.commit()
        cur.close()

    @classmethod
    def _remove_personnel_sql(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> tuple[str, Sequence[SQLiteType]]:
        consumable_where_query, consumable_values = cls._HANDLER.where_query(
            consumable_where
        )
        personnel_where_query, personnel_values = cls._HANDLER.where_query(
            personnel_where
        )
        roles_list = list(roles)
        role_placeholders = ", ".join(["?" for _ in range(len(roles))])

        sql = f"""
        DELETE FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id IN (
                    SELECT {to_shorthand(cls._HANDLER.TABLE_MAPPING[Consumable])}.id as consumable_id
                    FROM {cls._HANDLER.MEGATABLE_QUERY}
                    {consumable_where_query}
                )
            AND personnel_id IN
                (
                    SELECT {to_shorthand(cls._HANDLER.TABLE_MAPPING[Personnel])}.id as personnel_id
                    FROM {cls._HANDLER.MEGATABLE_QUERY}
                    {personnel_where_query}
                )
            AND role in ({role_placeholders})
        """

        return sql, consumable_values + personnel_values + roles_list

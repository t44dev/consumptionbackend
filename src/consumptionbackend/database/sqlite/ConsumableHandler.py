# stdlib
from collections import defaultdict
from collections.abc import MutableSequence, MutableSet, Sequence, MutableMapping, Set
import sqlite3
from typing import Unpack, final, override

# consumption
from consumptionbackend.database.queries import ApplyOperator, ApplyQuery
from consumptionbackend.database.fields import ConsumableApplyMapping
from consumptionbackend.entities import (
    Consumable,
    EntityRoles,
    Id,
    Personnel,
    Series,
)
from consumptionbackend.database import (
    ConsumableHandlerBase,
    ConsumableFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .sql_utils import SQLiteType, placeholders, to_shorthand


@final
class SQLiteConsumableHandler(ConsumableHandlerBase):

    _HANDLER = SQLiteDatabaseHandler

    @override
    @classmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Id:
        tags = values.pop("tags", [])

        new = cls._HANDLER.new(Consumable, **values)
        cls._change_tags(
            [new],
            [ApplyQuery(tag) for tag in tags],
        )

        return new

    @override
    @classmethod
    def find_by_id(cls, id: Id) -> Consumable:
        return cls._HANDLER.find_by_id(Consumable, id)

    @override
    @classmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[Consumable]:
        return cls._HANDLER.find_by_ids(Consumable, ids)

    @override
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        return cls._HANDLER.find(Consumable, **where)

    @override
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[Id]:
        tags = apply.pop("tags", [])
        # TODO: Avoid find
        consumables = cls.find(**where)

        updated = cls._HANDLER.update(Consumable, where, apply)
        cls._change_tags([c.id for c in consumables], tags)

        return updated

    @override
    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int:
        return cls._HANDLER.delete(Consumable, **where)

    @override
    @classmethod
    def series(cls, id: Id) -> Series:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        result: sqlite3.Row = cur.execute(*(cls._series_sql(id))).fetchone()

        cur.close()
        return Series(**result)

    @classmethod
    def _series_sql(cls, id: Id) -> tuple[str, Sequence[SQLiteType]]:
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

    @override
    @classmethod
    def personnel_by_id(cls, consumable_id: Id) -> Sequence[EntityRoles]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._personnel_by_id_sql(consumable_id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[Id, MutableSequence[str]] = defaultdict(list)
        for row in results:
            p_id = row["id"]
            role = row["role"]
            mapping[p_id].append(role)
        return [EntityRoles(p_id, mapping[p_id]) for p_id in mapping]

    @classmethod
    def _personnel_by_id_sql(cls, id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT personnel_id as id, role
            FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id = ?
        """

        return sql, [id]

    @override
    @classmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[Id]:
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

        # TODO: Avoid use find
        consumables = cls.find(**consumable_where)
        return [c.id for c in consumables]

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
            AND role in ({placeholders(len(roles))})
        """

        return sql, consumable_values + personnel_values + roles_list

    @classmethod
    def _change_tags(cls, ids: Sequence[Id], tags: Sequence[ApplyQuery[str]]) -> None:
        add_tags: MutableSet[str] = set()
        remove_tags: MutableSet[str] = set()

        for tag_query in tags:
            match tag_query.operator:
                case ApplyOperator.APPLY | ApplyOperator.ADD:
                    add_tags.add(tag_query.value)
                    if tag_query.value in remove_tags:
                        remove_tags.remove(tag_query.value)

                case ApplyOperator.SUB:
                    remove_tags.add(tag_query.value)
                    if tag_query.value in add_tags:
                        add_tags.remove(tag_query.value)

        if len(add_tags) > 0:
            cls._add_tags(ids, add_tags)
        if len(remove_tags) > 0:
            cls._remove_tags(ids, remove_tags)

    @classmethod
    def _add_tags(cls, ids: Sequence[Id], tags: Set[str]) -> None:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        _ = cur.execute(*(cls._add_tags_sql(ids, list(tags))))

        cls._HANDLER.PROVIDER().db.commit()
        cur.close()

    @classmethod
    def _add_tags_sql(
        cls, ids: Sequence[Id], tags: Sequence[str]
    ) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        INSERT OR IGNORE INTO {cls._HANDLER.TAGS_MAPPING_TABLE} (consumable_id, tag)
            SELECT * FROM
                (VALUES {placeholders(len(ids), "(?)")})
                CROSS JOIN
                (VALUES {placeholders(len(tags), "(?)")})
        """

        return sql, list(ids) + list(tags)

    @classmethod
    def _remove_tags(cls, ids: Sequence[Id], tags: Set[str]) -> None:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        _ = cur.execute(*(cls._remove_tags_sql(ids, list(tags))))

        cls._HANDLER.PROVIDER().db.commit()
        cur.close()

    @classmethod
    def _remove_tags_sql(
        cls, ids: Sequence[Id], tags: Sequence[str]
    ) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        DELETE FROM {cls._HANDLER.TAGS_MAPPING_TABLE}
            WHERE consumable_id IN ({placeholders(len(ids))})
            AND tag IN ({placeholders(len(tags))})
        """

        return sql, list(ids) + list(tags)

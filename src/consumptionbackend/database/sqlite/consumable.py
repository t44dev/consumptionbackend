import logging
import sqlite3
from collections import defaultdict
from collections.abc import MutableMapping, MutableSequence, MutableSet, Sequence, Set
from typing import Unpack, final, override

from consumptionbackend.database import (
    ConsumableFieldsRequired,
    ConsumableService,
    WhereMapping,
)
from consumptionbackend.database.fields import ConsumableApplyMapping
from consumptionbackend.database.queries import ApplyOperator, ApplyQuery
from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.entities import (
    Consumable,
    Id,
    IdRoles,
    Personnel,
    Series,
)
from consumptionbackend.utils import ServiceProvider

from .helper import SQLiteDatabaseHelper
from .sql_utils import SQLiteType, placeholders, to_shorthand

logger = logging.getLogger(__name__)


@final
class SQLiteConsumableService(ConsumableService):
    @override
    def new(self, **values: Unpack[ConsumableFieldsRequired]) -> Id:
        tags = values.pop("tags", [])

        new = SQLiteDatabaseHelper.new(Consumable, **values)
        self._change_tags(
            [new],
            [ApplyQuery(tag) for tag in tags],
        )

        return new

    @override
    def find_by_id(self, id: Id) -> Consumable:
        return SQLiteDatabaseHelper.find_by_id(Consumable, id)

    @override
    def find_by_ids(self, ids: Sequence[Id]) -> Sequence[Consumable]:
        return SQLiteDatabaseHelper.find_by_ids(Consumable, ids)

    @override
    def find(self, **where: Unpack[WhereMapping]) -> Sequence[Consumable]:
        return SQLiteDatabaseHelper.find(Consumable, **where)

    @override
    def update(
        self,
        where: WhereMapping,
        apply: ConsumableApplyMapping,
    ) -> Sequence[Id]:
        tags = apply.pop("tags", [])

        updated = SQLiteDatabaseHelper.update(Consumable, where, apply)
        self._change_tags(updated, tags)

        return updated

    @override
    def delete(self, **where: Unpack[WhereMapping]) -> int:
        return SQLiteDatabaseHelper.delete(Consumable, **where)

    @override
    def series(self, consumable_id: Id) -> Series:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        result: sqlite3.Row = cur.execute(*(self._series_sql(consumable_id))).fetchone()

        cur.close()

        series = Series(**result)
        logger.info(
            "Found Series for Consumable id",
            extra={
                "data": {"id": consumable_id, "result": series},
            },
        )

        return series

    def _series_sql(self, id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT * 
            FROM {SQLiteDatabaseHelper.TABLE_MAPPING[Series]} t1
            WHERE t1.id = (
                SELECT t2.series_id 
                    FROM {SQLiteDatabaseHelper.TABLE_MAPPING[Consumable]} t2
                    WHERE t2.id = ? 
            )
        """

        return sql, [id]

    @override
    def personnel(self, consumable_id: Id) -> Sequence[IdRoles]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(self._personnel_sql(consumable_id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[Id, MutableSequence[str]] = defaultdict(list)
        for row in results:
            p_id = row["id"]
            role = row["role"]
            mapping[p_id].append(role)

        id_roles = [IdRoles(p_id, mapping[p_id]) for p_id in mapping]
        logger.info(
            "Found Personnel for Consumable id",
            extra={
                "data": {"id": consumable_id, "results": id_roles},
            },
        )

        return id_roles

    def _personnel_sql(self, consumable_id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT personnel_id as id, role
            FROM {SQLiteDatabaseHelper.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id = ?
        """

        return sql, [consumable_id]

    @override
    def tags(self, consumable_id: Id) -> Sequence[str]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(self._tags_sql(consumable_id))
        ).fetchall()

        cur.close()

        tags = [row["tag"] for row in results]
        logger.info(
            "Found tags for Consumable id",
            extra={"data": {"id": consumable_id, "results": tags}},
        )

        return tags

    def _tags_sql(self, consumable_id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT tag
            FROM {SQLiteDatabaseHelper.TAGS_MAPPING_TABLE}
            WHERE consumable_id = ?
        """

        return sql, [consumable_id]

    @override
    def change_personnel(
        self,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[Id]:
        add_roles: MutableSet[str] = set()
        remove_roles: MutableSet[str] = set()

        # TODO: Avoid using find
        consumables = [c.id for c in self.find(**consumable_where)]

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
            self._add_personnel(consumable_where, personnel_where, add_roles)
        if len(remove_roles) > 0:
            self._remove_personnel(consumable_where, personnel_where, remove_roles)

        logger.info(
            "Changed Personnel for Consumable(s)",
            extra={
                "data": {
                    "consumable_query": consumable_where,
                    "personnel_query": personnel_where,
                    "roles": roles,
                    "updated_consumable_ids": consumables,
                }
            },
        )

        return consumables

    def _add_personnel(
        self,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> None:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        for role in roles:
            _ = cur.execute(
                *(self._add_personnel_sql(consumable_where, personnel_where, role))
            )

        db.commit()
        cur.close()

    def _add_personnel_sql(
        self,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        role: str,
    ) -> tuple[str, Sequence[SQLiteType]]:
        consumable_where_query, consumable_values = SQLiteDatabaseHelper.where_query(
            consumable_where
        )
        personnel_where_query, personnel_values = SQLiteDatabaseHelper.where_query(
            personnel_where
        )

        sql = f"""
        INSERT OR IGNORE INTO {SQLiteDatabaseHelper.PERSONNEL_MAPPING_TABLE} (consumable_id, personnel_id, role)
            SELECT * FROM 
                (
                    SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[Consumable])}.id as consumable_id
                    FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                    {consumable_where_query}
                )
                CROSS JOIN
                (
                    SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[Personnel])}.id as personnel_id
                    FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                    {personnel_where_query}
                )
                CROSS JOIN
                (
                    SELECT ? as role
                )
        """

        return sql, (*consumable_values, *personnel_values, role)

    def _remove_personnel(
        self,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> None:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        _ = cur.execute(
            *(self._remove_personnel_sql(consumable_where, personnel_where, roles))
        )

        db.commit()
        cur.close()

    def _remove_personnel_sql(
        self,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Set[str],
    ) -> tuple[str, Sequence[SQLiteType]]:
        consumable_where_query, consumable_values = SQLiteDatabaseHelper.where_query(
            consumable_where
        )
        personnel_where_query, personnel_values = SQLiteDatabaseHelper.where_query(
            personnel_where
        )
        roles_list = list(roles)

        sql = f"""
        DELETE FROM {SQLiteDatabaseHelper.PERSONNEL_MAPPING_TABLE}
            WHERE consumable_id IN (
                    SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[Consumable])}.id as consumable_id
                    FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                    {consumable_where_query}
                )
            AND personnel_id IN
                (
                    SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[Personnel])}.id as personnel_id
                    FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                    {personnel_where_query}
                )
            AND role IN ({placeholders(len(roles))})
        """

        return sql, [*consumable_values, *personnel_values, *roles_list]

    def _change_tags(self, ids: Sequence[Id], tags: Sequence[ApplyQuery[str]]) -> None:
        if len(ids) == 0:
            return

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
            self._add_tags(ids, add_tags)
        if len(remove_tags) > 0:
            self._remove_tags(ids, remove_tags)

        logger.info(
            "Changed tags for Consumable ids",
            extra={"data": {"ids": ids, "tags": tags}},
        )

    def _add_tags(self, ids: Sequence[Id], tags: Set[str]) -> None:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        _ = cur.execute(*(self._add_tags_sql(ids, list(tags))))

        db.commit()
        cur.close()

    def _add_tags_sql(
        self, ids: Sequence[Id], tags: Sequence[str]
    ) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        INSERT OR IGNORE INTO {SQLiteDatabaseHelper.TAGS_MAPPING_TABLE} (consumable_id, tag)
            SELECT * FROM
                (VALUES {placeholders(len(ids), "(?)")})
                CROSS JOIN
                (VALUES {placeholders(len(tags), "(?)")})
        """

        return sql, list(ids) + list(tags)

    def _remove_tags(self, ids: Sequence[Id], tags: Set[str]) -> None:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        _ = cur.execute(*(self._remove_tags_sql(ids, list(tags))))

        db.commit()
        cur.close()

    def _remove_tags_sql(
        self, ids: Sequence[Id], tags: Sequence[str]
    ) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        DELETE FROM {SQLiteDatabaseHelper.TAGS_MAPPING_TABLE}
            WHERE consumable_id IN ({placeholders(len(ids))})
            AND tag IN ({placeholders(len(tags))})
        """

        return sql, list(ids) + list(tags)

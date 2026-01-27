import logging
import sqlite3
from collections.abc import Mapping, MutableSequence, Sequence
from typing import Any, Unpack, final

from consumptionbackend.database import (
    ApplyQuery,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.entities import Consumable, EntityBase, Id, Personnel, Series
from consumptionbackend.utils import NotFoundError, ServiceProvider
from consumptionbackend.utils.exceptions import NoValuesError

from .sql_utils import (
    SQLiteType,
    fix_value,
    placeholders,
    to_shorthand,
    to_sqlite_operator,
    validate_column_name,
)

logger = logging.getLogger(__name__)


# TODO: Probably some more idiomatic way to do this with inheritance
@final
class SQLiteDatabaseHelper:
    TABLE_MAPPING = {
        EntityBase: "no_table",
        Consumable: "consumables",
        Series: "series",
        Personnel: "personnel",
    }

    TAGS_MAPPING_TABLE = "consumable_tags"
    PERSONNEL_MAPPING_TABLE = "consumable_personnel"

    # TODO: Refactor to avoid this
    MEGATABLE_QUERY = f"""
    {TABLE_MAPPING[Consumable]} {to_shorthand(TABLE_MAPPING[Consumable])} 
        FULL OUTER JOIN {TABLE_MAPPING[Series]} {to_shorthand(TABLE_MAPPING[Series])}
            ON {to_shorthand(TABLE_MAPPING[Series])}.id = {to_shorthand(TABLE_MAPPING[Consumable])}.series_id
        FULL OUTER JOIN {PERSONNEL_MAPPING_TABLE} {to_shorthand(PERSONNEL_MAPPING_TABLE)}
            ON {to_shorthand(PERSONNEL_MAPPING_TABLE)}.consumable_id = {to_shorthand(TABLE_MAPPING[Consumable])}.id
        FULL OUTER JOIN {TABLE_MAPPING[Personnel]} {to_shorthand(TABLE_MAPPING[Personnel])}
            ON {to_shorthand(TABLE_MAPPING[Personnel])}.id = {to_shorthand(PERSONNEL_MAPPING_TABLE)}.personnel_id
    """

    @classmethod
    def new[E: EntityBase](cls, t: type[E], **values: Any) -> Id:
        if len(values) == 0:
            raise NoValuesError(f"No values provided on creation of {t.__name__}.")

        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        id = cur.execute(*(cls._new_sql(t, **values))).lastrowid
        logger.info(
            "Created Entity",
            extra={"data": {"new": values, "entity": t.__name__, "id": id}},
        )

        if id is None:
            raise RuntimeError("No row id after insertion.")

        db.commit()
        cur.close()

        return id

    @classmethod
    def _new_sql[E: EntityBase](
        cls, t: type[E], **values: Any
    ) -> tuple[str, Sequence[SQLiteType]]:
        table = SQLiteDatabaseHelper.TABLE_MAPPING[t]

        new_values: Sequence[SQLiteType] = []

        labels: Sequence[str] = []
        for key, value in values.items():
            validate_column_name(key)
            new_values.append(fix_value(value))
            labels.append(key)
        labels_str = ", ".join(labels)

        sql = f"INSERT INTO {table} ({labels_str}) VALUES ({placeholders(len(values))})"

        return sql, new_values

    @classmethod
    def find_by_id[E: EntityBase](cls, t: type[E], id: Id) -> E:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        result: sqlite3.Row | None = cur.execute(
            *(cls._find_by_id_sql(t, id))
        ).fetchone()

        if result is None:
            raise NotFoundError(f"{t} with {id=} not found.")

        cur.close()

        entity = t(**result)
        logger.info(
            "Found Entity by id",
            extra={"data": {"id": id, "entity": t.__name__, "result": entity}},
        )

        return entity

    @classmethod
    def _find_by_id_sql[E: EntityBase](
        cls, t: type[E], id: Id
    ) -> tuple[str, Sequence[SQLiteType]]:
        table = SQLiteDatabaseHelper.TABLE_MAPPING[t]

        return f"SELECT * FROM {table} WHERE id = ?", [id]

    @classmethod
    def find_by_ids[E: EntityBase](cls, t: type[E], id: Sequence[Id]) -> Sequence[E]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._find_by_ids_sql(t, id))
        ).fetchall()

        cur.close()

        entities = [t(**result) for result in results]
        logger.info(
            "Found Entities by ids",
            extra={
                "data": {"ids": id, "entity": t.__name__, "results": entities},
            },
        )

        return entities

    @classmethod
    def _find_by_ids_sql[E: EntityBase](
        cls, t: type[E], ids: Sequence[Id]
    ) -> tuple[str, Sequence[SQLiteType]]:
        table = SQLiteDatabaseHelper.TABLE_MAPPING[t]

        return f"SELECT * FROM {table} WHERE id IN ({placeholders(len(ids))})", [*ids]

    @classmethod
    def find[E: EntityBase](
        cls, t: type[E], **where: Unpack[WhereMapping]
    ) -> Sequence[E]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._find_sql(t, **where))
        ).fetchall()

        cur.close()

        # TODO: Improve filtering of NONE row
        entities = [t(**result) for result in results if result["id"] is not None]
        logger.info(
            "Found Entities",
            extra={
                "data": {"query": where, "entity": t.__name__, "results": entities},
            },
        )

        return entities

    @classmethod
    def _find_sql[E: EntityBase](
        cls, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, Sequence[SQLiteType]]:
        where_query, values = SQLiteDatabaseHelper.where_query(where)

        sql = f"""
        SELECT DISTINCT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[t])}.* 
            FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
            {where_query}
        """

        return sql, values

    @classmethod
    def update[E: EntityBase](
        cls,
        t: type[E],
        where: WhereMapping,
        apply: Any,
    ) -> Sequence[Id]:
        if len(apply) == 0:
            return [e.id for e in cls.find(t, **where)]

        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._update_sql(t, where, apply))
        ).fetchall()

        db.commit()
        cur.close()

        ids = [row["id"] for row in results]
        logger.info(
            "Updated Entities",
            extra={
                "data": {
                    "query": where,
                    "apply": apply,
                    "entity": t.__name__,
                    "results": ids,
                },
            },
        )

        return ids

    @classmethod
    def _update_sql[E: EntityBase](
        cls,
        t: type[E],
        where: WhereMapping,
        apply: Any,
    ) -> tuple[str, Sequence[SQLiteType]]:
        where_query, where_values = SQLiteDatabaseHelper.where_query(where)
        apply_query, apply_values = SQLiteDatabaseHelper.apply_query(apply)

        sql = f"""
        UPDATE {SQLiteDatabaseHelper.TABLE_MAPPING[t]}
            SET {apply_query}
            WHERE id IN (
                SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[t])}.id 
                FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                {where_query}
            )
        RETURNING id
        """

        return sql, [*apply_values, *where_values]

    @classmethod
    def delete[E: EntityBase](cls, t: type[E], **where: Unpack[WhereMapping]) -> int:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._delete_sql(t, **where))
        ).fetchall()

        db.commit()
        cur.close()

        deleted = len(results)
        logger.info(
            "Deleted Entities",
            extra={
                "data": {"query": where, "entity": t.__name__, "deleted": deleted},
            },
        )

        return deleted

    @classmethod
    def _delete_sql[E: EntityBase](
        cls, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, Sequence[SQLiteType]]:
        where_query, values = SQLiteDatabaseHelper.where_query(where)

        sql = f"""
        DELETE FROM {SQLiteDatabaseHelper.TABLE_MAPPING[t]}
            WHERE id IN (
                SELECT {to_shorthand(SQLiteDatabaseHelper.TABLE_MAPPING[t])}.id 
                FROM {SQLiteDatabaseHelper.MEGATABLE_QUERY}
                {where_query}
            )
        RETURNING id
        """

        return sql, values

    @classmethod
    def where_query(cls, where: WhereMapping) -> tuple[str, Sequence[SQLiteType]]:
        where_list: Sequence[str] = []
        values: Sequence[SQLiteType] = []
        for table_name in where:
            mapping: Mapping[str, Any] = where.get(table_name, None)
            assert mapping is not None

            for column in mapping:
                validate_column_name(column)

                queries: Sequence[WhereQuery[Any]] = mapping[column]
                shorthand_table_name = (
                    to_shorthand(SQLiteDatabaseHelper.PERSONNEL_MAPPING_TABLE)
                    if column == "role"
                    else to_shorthand(table_name)
                )
                qualified_column = f"{shorthand_table_name}.{column}"

                # Tags handled uniquely
                if column == "tags":
                    tag_where, tag_values = cls.where_query_tags(queries)
                    if len(tag_values) > 0:
                        where_list.append(tag_where)
                        values = [*values, *tag_values]
                    continue

                for query in queries:
                    where_str, sub_value = to_sqlite_operator(qualified_column, query)
                    where_list.append(where_str)
                    values.append(sub_value)

        if len(where_list) > 0:
            return f"WHERE {' AND '.join(where_list)}", values
        return "", values

    @classmethod
    def where_query_tags(
        cls, queries: Sequence[WhereQuery[str]]
    ) -> tuple[str, Sequence[SQLiteType]]:
        eq_tags = list(
            map(
                lambda x: x.value,
                filter(lambda x: x.operator == WhereOperator.EQ, queries),
            )
        )
        neq_tags = list(
            map(
                lambda x: x.value,
                filter(lambda x: x.operator == WhereOperator.NEQ, queries),
            )
        )

        where: MutableSequence[str] = []
        values: MutableSequence[SQLiteType] = []
        if len(eq_tags) > 0:
            where.append(
                f"(SELECT COUNT(*) FROM {cls.TAGS_MAPPING_TABLE} ti WHERE {to_shorthand(cls.TABLE_MAPPING[Consumable])}.id = ti.consumable_id AND ti.tag IN ({', '.join('?' for _ in range(len(eq_tags)))})) = ?"
            )
            values = [*values, *eq_tags, len(eq_tags)]

        if len(neq_tags) > 0:
            where.append(
                f"NOT EXISTS (SELECT 1 FROM {cls.TAGS_MAPPING_TABLE} te WHERE {to_shorthand(cls.TABLE_MAPPING[Consumable])}.id = te.consumable_id AND te.tag IN ({', '.join('?' for _ in range(len(neq_tags)))}))"
            )
            values = [*values, *neq_tags]

        return " AND ".join(where), values

    @classmethod
    def apply_query(cls, apply: Any) -> tuple[str, Sequence[SQLiteType]]:
        apply_list: Sequence[str] = []
        values: Sequence[SQLiteType] = []
        for column in apply:
            validate_column_name(column)

            query: ApplyQuery[Any] = apply[column]

            apply_str, sub_value = to_sqlite_operator(column, query)
            apply_list.append(apply_str)
            values.append(sub_value)

        return ", ".join(apply_list), values

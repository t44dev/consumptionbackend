# stdlib
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any, TypeVar, Unpack, final


# consumption
from .sql_utils import (
    SQLiteType,
    fix_value,
    placeholders,
    to_shorthand,
    to_sqlite_operator,
    validate_column_name,
)
from consumptionbackend.entities import Consumable, Id, Series, Personnel
from consumptionbackend.database import (
    WhereMapping,
    ApplyQuery,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import EntityBase
from .database_provider import SQLiteDatabaseProviderBase, SQLiteFileDatabaseProvider

E = TypeVar("E", bound=EntityBase)


@final
class SQLiteDatabaseHandler:

    PROVIDER: type[SQLiteDatabaseProviderBase] = SQLiteFileDatabaseProvider

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
    def new(cls, t: type[E], **values: Any) -> Id:
        cur = cls.PROVIDER().db.cursor()

        id = cur.execute(*(cls._new_sql(t, **values))).lastrowid

        if id is None:
            raise RuntimeError("No row id after insertion.")

        cls.PROVIDER().db.commit()
        cur.close()

        return id

    @classmethod
    def _new_sql(cls, t: type[E], **values: Any) -> tuple[str, list[SQLiteType]]:
        table = SQLiteDatabaseHandler.TABLE_MAPPING[t]

        new_values: list[SQLiteType] = []

        labels: list[str] = []
        for key, value in values.items():
            validate_column_name(key)
            new_values.append(fix_value(value))
            labels.append(key)
        labels_str = ", ".join(labels)

        sql = f"INSERT INTO {table} ({labels_str}) VALUES ({placeholders(len(values))})"

        return sql, new_values

    @classmethod
    def find_by_id(cls, t: type[E], id: Id) -> E:
        cur = cls.PROVIDER().db.cursor()

        result: sqlite3.Row | None = cur.execute(
            *(cls._find_by_id_sql(t, id))
        ).fetchone()

        if result is None:
            raise RuntimeError("No result on find by id.")

        cur.close()

        return t(**result)

    @classmethod
    def _find_by_id_sql(cls, t: type[E], id: Id) -> tuple[str, list[SQLiteType]]:
        table = SQLiteDatabaseHandler.TABLE_MAPPING[t]

        return f"SELECT * FROM {table} WHERE id = ?", [id]

    @classmethod
    def find_by_ids(cls, t: type[E], id: Sequence[Id]) -> Sequence[E]:
        cur = cls.PROVIDER().db.cursor()

        results: list[sqlite3.Row] = cur.execute(
            *(cls._find_by_ids_sql(t, id))
        ).fetchall()

        cur.close()

        return [t(**result) for result in results]

    @classmethod
    def _find_by_ids_sql(
        cls, t: type[E], ids: Sequence[Id]
    ) -> tuple[str, list[SQLiteType]]:
        table = SQLiteDatabaseHandler.TABLE_MAPPING[t]

        return f"SELECT * FROM {table} WHERE id IN ({placeholders(len(ids))})", [*ids]

    @classmethod
    def find(cls, t: type[E], **where: Unpack[WhereMapping]) -> Sequence[E]:
        cur = cls.PROVIDER().db.cursor()

        results: list[sqlite3.Row] = cur.execute(
            *(cls._find_sql(t, **where))
        ).fetchall()

        cur.close()

        return [t(**result) for result in results]

    @classmethod
    def _find_sql(
        cls, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, list[SQLiteType]]:
        where_query, values = SQLiteDatabaseHandler.where_query(where)

        sql = f"""
        SELECT DISTINCT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.* 
            FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
            {where_query}
        """

        return sql, values

    @classmethod
    def update(
        cls,
        t: type[E],
        where: WhereMapping,
        apply: Any,
    ) -> Sequence[Id]:
        if len(apply) == 0:
            return []

        cur = cls.PROVIDER().db.cursor()

        ids: list[Id] = cur.execute(*(cls._update_sql(t, where, apply))).fetchall()

        cls.PROVIDER().db.commit()
        cur.close()

        return ids

    @classmethod
    def _update_sql(
        cls,
        t: type[E],
        where: WhereMapping,
        apply: Any,
    ) -> tuple[str, list[SQLiteType]]:
        where_query, where_values = SQLiteDatabaseHandler.where_query(where)
        apply_query, apply_values = SQLiteDatabaseHandler.apply_query(apply)

        sql = f"""
        UPDATE {SQLiteDatabaseHandler.TABLE_MAPPING[t]}
            SET {apply_query}
            WHERE id IN (
                SELECT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.id 
                FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
                {where_query}
            )
        RETURNING {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.id
        """

        return sql, (apply_values + where_values)

    @classmethod
    def delete(cls, t: type[E], **where: Unpack[WhereMapping]) -> int:
        cur = cls.PROVIDER().db.cursor()

        result: int = cur.execute(*(cls._delete_sql(t, **where))).fetchone()

        cls.PROVIDER().db.commit()
        cur.close()

        return result

    @classmethod
    def _delete_sql(
        cls, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, list[SQLiteType]]:
        where_query, values = SQLiteDatabaseHandler.where_query(where)

        sql = f"""
        SELECT COUNT(*) FROM (
            DELETE FROM {SQLiteDatabaseHandler.TABLE_MAPPING[t]}
                WHERE id IN (
                    SELECT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.id 
                    FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
                    {where_query}
                )
            RETURNING 1
            )
        """

        return sql, values

    @classmethod
    def where_query(cls, where: WhereMapping) -> tuple[str, list[SQLiteType]]:
        where_list: list[str] = []
        values: list[SQLiteType] = []
        for table_name in where:
            mapping: Mapping[str, Any] = where.get(table_name, None)
            assert mapping is not None

            for column in mapping:
                validate_column_name(column)

                queries: list[WhereQuery[Any]] = mapping[column]
                shorthand_table_name = (
                    to_shorthand(SQLiteDatabaseHandler.PERSONNEL_MAPPING_TABLE)
                    if column == "role"
                    else to_shorthand(table_name)
                )
                qualified_column = f"{shorthand_table_name}.{column}"

                # Tags must be ORed over and so are handled uniquely with "IN"
                if column == "tags":
                    tag_where, tag_values = cls.where_query_tags(queries)
                    if len(tag_values) > 0:
                        where_list.append(tag_where)
                        values = values + tag_values
                    continue

                for query in queries:
                    where_str, sub_value = to_sqlite_operator(qualified_column, query)
                    where_list.append(where_str)
                    values.append(sub_value)

        if len(where_list) > 0:
            return f"WHERE {' AND '.join(where_list)}", values
        return "", values

    @classmethod
    def where_query_tags(cls, queries: list[WhereQuery[str]]) -> tuple[str, list[str]]:
        tag_shorthand = "tgw"
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

        tags_where: list[str] = []
        if len(eq_tags) > 0:
            tags_where.append(
                f"{tag_shorthand}.tag IN ({', '.join('?' for _ in range(len(eq_tags)))})"
            )
        if len(neq_tags) > 0:
            tags_where.append(
                f"{tag_shorthand}.tag NOT IN ({', '.join('?' for _ in range(len(neq_tags)))})"
            )

        tag_where = f"""
            {to_shorthand(cls.TABLE_MAPPING[Consumable])}.id IN (
                SELECT {tag_shorthand}.consumable_id FROM {cls.TAGS_MAPPING_TABLE} {tag_shorthand}
                WHERE {' AND '.join(tags_where)}
            )
            """

        return tag_where, (eq_tags + neq_tags)

    @classmethod
    def apply_query(cls, apply: Any) -> tuple[str, list[SQLiteType]]:
        apply_list: list[str] = []
        values: list[SQLiteType] = []
        for column in apply:
            validate_column_name(column)

            query: ApplyQuery[Any] = apply[column]

            apply_str, sub_value = to_sqlite_operator(column, query)
            apply_list.append(apply_str)
            values.append(sub_value)

        return ", ".join(apply_list), values

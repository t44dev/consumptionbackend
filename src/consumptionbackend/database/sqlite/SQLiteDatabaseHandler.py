# stdlib
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any, TypeVar, Unpack, final


# consumption
from .sql_utils import (
    SQLiteType,
    fix_value,
    to_shorthand,
    to_sqlite_operator,
    validate_column_name,
)
from consumptionbackend.database import DatabaseProviderBase
from consumptionbackend.entities import Consumable, Series, Personnel
from consumptionbackend.database import (
    ApplyMapping,
    DatabaseHandlerBase,
    WhereMapping,
    ApplyQuery,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import EntityBase
from .database_provider import SQLiteFileDatabaseProvider

E = TypeVar("E", bound=EntityBase)


@final
class SQLiteDatabaseHandler(DatabaseHandlerBase):

    _PROVIDER: type[DatabaseProviderBase] = SQLiteFileDatabaseProvider

    TABLE_MAPPING = {
        EntityBase: "no_table",
        Consumable: "consumables",
        Series: "series",
        Personnel: "personnel",
    }

    TAGS_MAPPING_TABLE = "consumable_tags"
    PERSONNEL_MAPPING_TABLE = "consumable_personnel"

    # TODO: Don't do this probably
    MEGATABLE_QUERY = f"""
    {TABLE_MAPPING[Consumable]} {to_shorthand(TABLE_MAPPING[Consumable])} 
        JOIN {TABLE_MAPPING[Series]} {to_shorthand(TABLE_MAPPING[Series])}
            ON {to_shorthand(TABLE_MAPPING[Series])}.id = {to_shorthand(TABLE_MAPPING[Consumable])}.series_id
        JOIN {PERSONNEL_MAPPING_TABLE} {to_shorthand(PERSONNEL_MAPPING_TABLE)}
            ON {to_shorthand(PERSONNEL_MAPPING_TABLE)}.consumable_id = {to_shorthand(TABLE_MAPPING[Consumable])}.id
        JOIN {TABLE_MAPPING[Personnel]} {to_shorthand(TABLE_MAPPING[Personnel])}
            ON {to_shorthand(TABLE_MAPPING[Personnel])}.id = {to_shorthand(PERSONNEL_MAPPING_TABLE)}.personnel_id
    """

    def __init__(self) -> None:
        super().__init__()
        self.db = SQLiteDatabaseHandler._PROVIDER.setup()
        assert isinstance(self.db, sqlite3.Connection)
        self.db.row_factory = sqlite3.Row

    def new(self, t: type[E], **values: Any) -> E:
        cur = self.db.cursor()

        row = cur.execute(*(self._new_sql(t, **values))).lastrowid

        if row is None:
            raise RuntimeError("No row id after insertion.")

        self.db.commit()
        cur.close()
        return self.find_by_id(t, row)

    def _new_sql(self, t: type[E], **values: Any) -> tuple[str, list[SQLiteType]]:
        table = SQLiteDatabaseHandler.TABLE_MAPPING[t]
        placeholders = ", ".join(["?" for _ in range(len(values))])

        new_values: list[SQLiteType] = []

        labels: list[str] = []
        for key, value in values.items():
            validate_column_name(key)
            new_values.append(fix_value(value))
            labels.append(key)
        labels_str = ", ".join(labels)

        sql = f"INSERT INTO {table} ({labels_str}) VALUES ({placeholders})"

        return sql, new_values

    def find_by_id(self, t: type[E], id: int) -> E:
        cur = self.db.cursor()

        result: sqlite3.Row | None = cur.execute(
            *(self._find_by_id_sql(t, id))
        ).fetchone()

        if result is None:
            raise RuntimeError("No result on find by id.")

        cur.close()
        return t(**result)

    def _find_by_id_sql(self, t: type[E], id: int) -> tuple[str, list[SQLiteType]]:
        table = SQLiteDatabaseHandler.TABLE_MAPPING[t]

        return f"SELECT * FROM {table} WHERE id = ?", [id]

    def find(self, t: type[E], **where: Unpack[WhereMapping]) -> Sequence[E]:
        cur = self.db.cursor()

        results: list[sqlite3.Row] = cur.execute(
            *(self._find_sql(t, **where))
        ).fetchall()

        self.db.commit()
        cur.close()
        return list(map(lambda result: t(**result), results))

    def _find_sql(
        self, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, list[SQLiteType]]:
        where_query, values = SQLiteDatabaseHandler.where_query(where)

        sql = f"""
        SELECT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.* 
            FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
            WHERE {where_query}
        """

        return sql, values

    def update(
        self,
        t: type[E],
        where: WhereMapping,
        apply: ApplyMapping,
    ) -> Sequence[E]:
        cur = self.db.cursor()

        results: list[sqlite3.Row] = cur.execute(
            *(self._update_sql(t, where, apply))
        ).fetchall()

        self.db.commit()
        cur.close()
        return list(map(lambda result: t(**result), results))

    def _update_sql(
        self,
        t: type[E],
        where: WhereMapping,
        apply: ApplyMapping,
    ) -> tuple[str, list[SQLiteType]]:
        where_query, where_values = SQLiteDatabaseHandler.where_query(where)
        apply_query, apply_values = SQLiteDatabaseHandler.apply_query(apply)

        sql = f"""
        UPDATE {SQLiteDatabaseHandler.TABLE_MAPPING[t]}
            SET {apply_query}
            WHERE id IN (
                SELECT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.id 
                FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
                WHERE {where_query}
            )
        RETURNING *
        """

        return sql, (apply_values + where_values)

    def delete(self, t: type[E], **where: Unpack[WhereMapping]) -> None:
        cur = self.db.cursor()

        _ = cur.execute(*(self._delete_sql(t, **where)))

        self.db.commit()
        cur.close()

    def _delete_sql(
        self, t: type[E], **where: Unpack[WhereMapping]
    ) -> tuple[str, list[SQLiteType]]:
        where_query, values = SQLiteDatabaseHandler.where_query(where)

        sql = f"""
        DELETE FROM {SQLiteDatabaseHandler.TABLE_MAPPING[t]} t
            WHERE t.id IN (
                SELECT {to_shorthand(SQLiteDatabaseHandler.TABLE_MAPPING[t])}.id 
                FROM {SQLiteDatabaseHandler.MEGATABLE_QUERY}
                WHERE {where_query}
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
                # TODO: Can this be avoided?
                shorthand_table_name = (
                    to_shorthand(table_name)
                    if column != "role"
                    else to_shorthand(SQLiteDatabaseHandler.PERSONNEL_MAPPING_TABLE)
                )
                qualified_column = f"{shorthand_table_name}.{column}"

                # Tags are a unique case
                if column == "tag":
                    tag_where, tag_values = cls.where_query_tags(queries)
                    where_list.append(tag_where)
                    values = values + tag_values
                    continue

                for query in queries:
                    where_str, sub_value = to_sqlite_operator(qualified_column, query)
                    where_list.append(where_str)
                    values.append(sub_value)

        return " AND ".join(where_list), values

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

        assert len(eq_tags) > 0 or len(neq_tags) > 0

        tags_where: list[str] = []
        if len(eq_tags) > 0:
            tags_where.append(
                f"{tag_shorthand}.tag IN ({' '.join('?' for _ in range(len(eq_tags)))})"
            )
        if len(neq_tags) > 0:
            tags_where.append(
                f"{tag_shorthand}.tag NOT IN ({' '.join('?' for _ in range(len(neq_tags)))})"
            )

        tag_where = f"""
            {to_shorthand(cls.TABLE_MAPPING[Consumable])}.id IN (
                SELECT {tag_shorthand}.consumable_id FROM {cls.TAGS_MAPPING_TABLE} {tag_shorthand}
                WHERE {' AND '.join(tags_where)}
            )
            """

        return tag_where, (eq_tags + neq_tags)

    @classmethod
    def apply_query(cls, apply: ApplyMapping) -> tuple[str, list[SQLiteType]]:
        apply_list: list[str] = []
        values: list[SQLiteType] = []
        for column in apply:
            validate_column_name(column)

            query: ApplyQuery[Any] = apply[column]

            apply_str, sub_value = to_sqlite_operator(column, query)
            apply_list.append(apply_str)
            values.append(sub_value)

        return ", ".join(apply_list), values

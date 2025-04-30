# stdlib
from collections import defaultdict
import sqlite3
from typing import Unpack, final
from collections.abc import MutableMapping, MutableSequence, Sequence

# consumption
from consumptionbackend.database.fields import PersonnelApplyMapping
from consumptionbackend.entities import Personnel, ConsumableRoles
from consumptionbackend.database import (
    PersonnelHandlerBase,
    PersonnelFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .ConsumableHandler import SQLiteConsumableHandler
from .sql_utils import SQLiteType


@final
class SQLitePersonnelHandler(PersonnelHandlerBase):

    _HANDLER = SQLiteDatabaseHandler

    @classmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        return cls._HANDLER.new(Personnel, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Personnel:
        return cls._HANDLER.find_by_id(Personnel, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        return cls._HANDLER.find(Personnel, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[Personnel]:
        return cls._HANDLER.update(Personnel, where, apply)

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls._HANDLER.delete(Personnel, **where)

    @classmethod
    def consumables(cls, id: int) -> Sequence[ConsumableRoles]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._consumables_sql(id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[int, MutableSequence[str]] = defaultdict(list)
        for row in results:
            c_id = row["id"]
            role = row["role"]
            mapping[c_id].append(role)
        return [
            ConsumableRoles(SQLiteConsumableHandler.find_by_id(c_id), mapping[c_id])
            for c_id in mapping
        ]

    @classmethod
    def _consumables_sql(cls, id: int) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT consumable_id as id, role
            FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE personnel_id = ?
        """

        return sql, [id]

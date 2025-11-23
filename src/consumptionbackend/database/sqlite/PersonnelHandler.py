# stdlib
from collections import defaultdict
import sqlite3
from typing import Unpack, final, override
from collections.abc import MutableMapping, MutableSequence, Sequence

# consumption
from consumptionbackend.database.fields import PersonnelApplyMapping
from consumptionbackend.entities import EntityRoles, Id, Personnel
from consumptionbackend.database import (
    PersonnelHandlerBase,
    PersonnelFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from .sql_utils import SQLiteType


@final
class SQLitePersonnelHandler(PersonnelHandlerBase):

    _HANDLER = SQLiteDatabaseHandler

    @override
    @classmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Id:
        return cls._HANDLER.new(Personnel, **values)

    @override
    @classmethod
    def find_by_id(cls, id: Id) -> Personnel:
        return cls._HANDLER.find_by_id(Personnel, id)

    @override
    @classmethod
    def find_by_ids(cls, ids: Sequence[Id]) -> Sequence[Personnel]:
        return cls._HANDLER.find_by_ids(Personnel, ids)

    @override
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        return cls._HANDLER.find(Personnel, **where)

    @override
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[Id]:
        return cls._HANDLER.update(Personnel, where, apply)

    @override
    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> int:
        return cls._HANDLER.delete(Personnel, **where)

    @override
    @classmethod
    def consumables_by_id(cls, personnel_id: Id) -> Sequence[EntityRoles]:
        cur = cls._HANDLER.PROVIDER().db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(cls._consumables_by_id_sql(personnel_id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[Id, MutableSequence[str]] = defaultdict(list)
        for row in results:
            c_id = row["id"]
            role = row["role"]
            mapping[c_id].append(role)
        return [EntityRoles(c_id, mapping[c_id]) for c_id in mapping]

    @classmethod
    def _consumables_by_id_sql(
        cls, personnel_id: Id
    ) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT consumable_id as id, role
            FROM {cls._HANDLER.PERSONNEL_MAPPING_TABLE}
            WHERE personnel_id = ?
        """

        return sql, [personnel_id]

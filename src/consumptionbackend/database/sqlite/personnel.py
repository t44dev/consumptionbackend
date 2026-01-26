import logging
import sqlite3
from collections import defaultdict
from collections.abc import MutableMapping, MutableSequence, Sequence
from typing import Unpack, final, override

from consumptionbackend.database import (
    PersonnelFieldsRequired,
    PersonnelService,
    WhereMapping,
)
from consumptionbackend.database.fields import PersonnelApplyMapping
from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.entities import Id, IdRoles, Personnel
from consumptionbackend.utils import ServiceProvider

from .helper import SQLiteDatabaseHelper
from .sql_utils import SQLiteType

logger = logging.getLogger(__name__)


@final
class SQLitePersonnelService(PersonnelService):
    @override
    def new(self, **values: Unpack[PersonnelFieldsRequired]) -> Id:
        return SQLiteDatabaseHelper.new(Personnel, **values)

    @override
    def find_by_id(self, id: Id) -> Personnel:
        return SQLiteDatabaseHelper.find_by_id(Personnel, id)

    @override
    def find_by_ids(self, ids: Sequence[Id]) -> Sequence[Personnel]:
        return SQLiteDatabaseHelper.find_by_ids(Personnel, ids)

    @override
    def find(self, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        return SQLiteDatabaseHelper.find(Personnel, **where)

    @override
    def update(
        self,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[Id]:
        return SQLiteDatabaseHelper.update(Personnel, where, apply)

    @override
    def delete(self, **where: Unpack[WhereMapping]) -> int:
        return SQLiteDatabaseHelper.delete(Personnel, **where)

    @override
    def consumables(self, personnel_id: Id) -> Sequence[IdRoles]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(
            *(self._consumables_sql(personnel_id))
        ).fetchall()

        cur.close()

        mapping: MutableMapping[Id, MutableSequence[str]] = defaultdict(list)
        for row in results:
            c_id = row["id"]
            role = row["role"]
            mapping[c_id].append(role)

        id_roles = [IdRoles(c_id, mapping[c_id]) for c_id in mapping]
        logger.info(
            "Found Consumables for Personnel id",
            extra={
                "data": {"id": personnel_id, "results": id_roles},
            },
        )

        return id_roles

    def _consumables_sql(self, personnel_id: Id) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT consumable_id as id, role
            FROM {SQLiteDatabaseHelper.PERSONNEL_MAPPING_TABLE}
            WHERE personnel_id = ?
        """

        return sql, [personnel_id]

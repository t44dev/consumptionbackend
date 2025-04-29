# stdlib
from typing import Unpack, final
from collections.abc import Sequence

# consumption
from consumptionbackend.database.fields import PersonnelApplyMapping
from consumptionbackend.entities import Personnel
from consumptionbackend.database import (
    PersonnelHandlerBase,
    PersonnelFieldsRequired,
    WhereMapping,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler


@final
class SQLitePersonnelHandlerBase(PersonnelHandlerBase):

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

# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, cast

# consumption
from .database_handling import (
    ApplyMapping,
    DatabaseHandlerBase,
    EntityHandlerBase,
    WhereMapping,
)
from .fields import (
    PersonnelApplyMapping,
    PersonnelFieldsRequired,
    PersonnelWhereMapping,
)
from consumptionbackend.entities import Personnel


class PersonnelHandlerBase(EntityHandlerBase, ABC):

    _HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        return cls._HANDLER().new(Personnel, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Personnel:
        return cls._HANDLER().find_by_id(Personnel, id)

    @classmethod
    def find(cls, **where: Unpack[PersonnelWhereMapping]) -> Sequence[Personnel]:
        return cls._HANDLER().find(Personnel, **where)

    @classmethod
    def update(
        cls,
        where: PersonnelWhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[Personnel]:
        return cls._HANDLER().update(
            Personnel,
            cast(WhereMapping, cast(object, where)),
            cast(ApplyMapping, apply),
        )

    @classmethod
    def delete(cls, **where: Unpack[PersonnelWhereMapping]) -> None:
        return cls._HANDLER().delete(Personnel, **where)

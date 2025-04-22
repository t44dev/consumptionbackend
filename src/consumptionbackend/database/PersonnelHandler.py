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
)
from consumptionbackend.entities import Personnel


class PersonnelHandlerBase(EntityHandlerBase, ABC):

    HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        return cls.HANDLER().new(Personnel, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Personnel:
        return cls.HANDLER().find_by_id(Personnel, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        return cls.HANDLER().find(Personnel, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[Personnel]:
        return cls.HANDLER().update(Personnel, where, cast(ApplyMapping, apply))

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls.HANDLER().delete(Personnel, **where)

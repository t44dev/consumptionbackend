# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack

# consumption
from .database_handling import EntityHandlerBase, WhereMapping
from .fields import (
    PersonnelApplyMapping,
    PersonnelFieldsRequired,
)
from consumptionbackend.entities import Personnel


class PersonnelHandlerBase(EntityHandlerBase, ABC):

    @classmethod
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls, id: int) -> Personnel:
        pass

    @classmethod
    @abstractmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        pass

    @classmethod
    @abstractmethod
    def update(
        cls,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[int]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

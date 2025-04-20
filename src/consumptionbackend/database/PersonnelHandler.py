# stdlib

# consumption
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack
from consumptionbackend.database.base_handlers import EntityHandlerBase, WhereMapping
from consumptionbackend.database.fields import (
    PersonnelApplyMapping,
    PersonnelFieldsRequired,
)
from consumptionbackend.entities import Personnel


class PersonnelHandlerBase(EntityHandlerBase, ABC):

    @abstractmethod
    @classmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Personnel:
        pass

    @abstractmethod
    @classmethod
    def find_by_id(cls, id: int) -> Personnel:
        pass

    @abstractmethod
    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Personnel]:
        pass

    @abstractmethod
    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: PersonnelApplyMapping,
    ) -> Sequence[int]:
        pass

    @abstractmethod
    @classmethod
    def delete(cls, **where: WhereMapping) -> None:
        pass

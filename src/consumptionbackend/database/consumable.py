from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import (
    Consumable,
    Id,
    IdRoles,
    Series,
)
from consumptionbackend.utils import ServiceBase

from .base import EntityServiceBase
from .fields import ConsumableFieldsRequired, WhereMapping
from .queries import ApplyQuery


class ConsumableService(EntityServiceBase[Consumable], ServiceBase, ABC):
    @override
    @abstractmethod
    def new(cls, **values: Unpack[ConsumableFieldsRequired]) -> Id: ...

    @abstractmethod
    def series(cls, consumable_id: Id) -> Series: ...

    @abstractmethod
    def personnel(cls, consumable_id: Id) -> Sequence[IdRoles]: ...

    @abstractmethod
    def tags(cls, consumable_id: Id) -> Sequence[str]: ...

    @abstractmethod
    def change_personnel(
        cls,
        consumable_where: WhereMapping,
        personnel_where: WhereMapping,
        roles: Sequence[ApplyQuery[str]],
    ) -> Sequence[Id]: ...

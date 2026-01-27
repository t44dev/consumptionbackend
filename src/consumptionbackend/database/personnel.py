from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, override

from consumptionbackend.entities import Id, IdRoles, Personnel
from consumptionbackend.utils import ServiceBase

from .base import EntityServiceBase
from .fields import PersonnelFieldsRequired


class PersonnelService(EntityServiceBase[Personnel], ServiceBase, ABC):
    @override
    @abstractmethod
    def new(cls, **values: Unpack[PersonnelFieldsRequired]) -> Id: ...

    @abstractmethod
    def consumables(cls, personnel_id: Id) -> Sequence[IdRoles]: ...

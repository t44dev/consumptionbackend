from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NamedTuple, override

from .types import Id


@dataclass
class EntityBase(ABC):
    id: Id

    @override
    def __hash__(self) -> int:
        return hash(self.id)


class IdRoles(NamedTuple):
    id: Id
    roles: Sequence[str]

# stdlib
from abc import ABC
from dataclasses import dataclass


@dataclass
class EntityBase(ABC):
    id: int

    def __hash__(self) -> int:
        return hash(self.id)

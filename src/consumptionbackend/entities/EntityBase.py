# stdlib
from abc import ABC
from dataclasses import dataclass


@dataclass
class EntityBase(ABC):
    id: int

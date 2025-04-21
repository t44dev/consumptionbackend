# stdlib
from dataclasses import dataclass

# consumption
from .EntityBase import EntityBase


@dataclass
class Series(EntityBase):
    name: str

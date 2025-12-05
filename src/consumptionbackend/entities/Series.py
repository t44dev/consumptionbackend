from dataclasses import dataclass

from .EntityBase import EntityBase


@dataclass
class Series(EntityBase):
    name: str

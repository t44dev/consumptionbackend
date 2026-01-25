from dataclasses import dataclass

from .base import EntityBase


@dataclass
class Series(EntityBase):
    name: str

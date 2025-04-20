# stdlib
from dataclasses import dataclass

# consumption
from consumptionbackend.entities.EntityBase import EntityBase


@dataclass
class Series(EntityBase):
    name: str

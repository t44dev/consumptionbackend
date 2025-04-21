# stdlib
from dataclasses import dataclass
from typing import NamedTuple

# consumption
from .EntityBase import EntityBase


@dataclass
class Personnel(EntityBase):
    first_name: str | None
    last_name: str | None
    pseudonym: str | None


class PersonnelRole(NamedTuple):
    personnel: Personnel
    role: str

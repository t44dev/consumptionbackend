# stdlib
from dataclasses import dataclass
from typing import NamedTuple
from collections.abc import Sequence

# consumption
from .EntityBase import EntityBase


@dataclass
class Personnel(EntityBase):
    first_name: str | None
    last_name: str | None
    pseudonym: str | None


class PersonnelRoles(NamedTuple):
    personnel: Personnel
    role: Sequence[str]

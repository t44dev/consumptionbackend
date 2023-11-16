# General Imports
from __future__ import annotations
from collections.abc import Mapping, Sequence  # For self-referential type-hints
from typing import Union, Any

# Package Imports
from .Database import DatabaseEntity


class Personnel(DatabaseEntity):

    DB_NAME = "personnel"

    def __init__(self, *args,
                 id: Union[int, None] = None,
                 first_name: Union[str, None] = None,
                 last_name: Union[str, None] = None,
                 pseudonym: Union[str, None] = None,
                 role: Union[str, None] = None) -> None:
        super().__init__(id)
        self.first_name = first_name
        self.last_name = last_name
        self.pseudonym = pseudonym
        self.role = role

    @classmethod
    def new(cls, **kwargs) -> DatabaseEntity:
        return super().new(**kwargs)

    @classmethod
    def find(cls, **kwargs) -> Sequence[DatabaseEntity]:
        return super().find(**kwargs)

    @classmethod
    def update(cls, where: Mapping[str, Any], set: Mapping[str, Any]) -> Sequence[DatabaseEntity]:
        return super().update(where, set)

    @classmethod
    def delete(cls, **kwargs) -> bool:
        return super().delete(**kwargs)

    def __str__(self) -> str:
        # TODO: Make this be in the format First "Pseudonym" Last, omitting NoneTypes
        return f"{self.__class__.__name__} | {self.first_name} {self.last_name} with ID: {self.id}"

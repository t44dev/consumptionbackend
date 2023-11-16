# General Imports
from collections.abc import Mapping, Any, Sequence
from typing import Union

# Package Imports
from .Database import DatabaseEntity
from .Consumable import Consumable


class Series(DatabaseEntity):

    def __init__(self, *args,
                 id: Union[int, None] = None,
                 name: str = "") -> None:
        super().__init__(*args, id=id)
        self.name = name

    def get_consumables(self) -> Sequence[Consumable]:
        pass

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
        return f"{self.__class__.__name__} | {self.name} with ID: {self.id}"

# General Imports
from __future__ import annotations  # For self-referential type-hints
from typing import Union, Any
from datetime import datetime
from enum import Enum
from collections.abc import Sequence, Mapping

# Package Imports
from .Database import DatabaseEntity
from .Personnel import Personnel
from .Series import Series


class Status(Enum):
    PLANNING = 0
    IN_PROGRESS = 1
    ON_HOLD = 2
    DROPPED = 3
    COMPLETED = 4


class Consumable(DatabaseEntity):

    DB_NAME = "consumables"
    DB_PERSONNEL_MAPPING_NAME = "consumable_personnel"

    def __init__(self, *args,
                 id: Union[int, None] = None,
                 name: str = "",
                 type: str = "",
                 status: Union[Status, int] = Status.PLANNING,
                 parts: int = 0,
                 completions: int = 0,
                 rating: Union[float, None] = None,
                 start_date: Union[float, None] = None,
                 end_date: Union[float, None] = None) -> None:
        super().__init__(id)
        self.name = name
        self.type = type.upper()
        self.status = status
        self.parts = parts
        self.completions = completions
        self.rating = rating
        self.start_date = start_date
        self.end_date = end_date
        self._enforce_constraints()

    def _enforce_constraints(self) -> None:
        # Conversions
        # Convert status to Enum
        if not isinstance(self.status, Status):
            self.status = Status(self.status)
        # Set completions if completed
        if self.completions == 0 and self.status == Status.COMPLETED:
            self.completions = 1
        # Change to in progress if a start_date is set
        if self.start_date is None and self.status == Status.IN_PROGRESS:
            self.start_date = datetime.utcnow().timestamp()     # Posix-timestamp
        # Minor >= Major
        if self.minor_parts < self.major_parts:
            self.minor_parts = self.major_parts
        # Major == 1 on COMPLETE
        if self.major_parts == 0 and self.status == Status.COMPLETED:
            self.major_parts = 1
        # Errors
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("End date must be after start date.")

    def get_personnel(self) -> Sequence[Personnel]:
        pass

    def get_series(self) -> Series:
        pass

    @classmethod
    def _assert_attrs(cls, d : Mapping[str, Any]) -> None:
        attrs = { "id", "name", "type", "status", "parts", "completions", "rating", "start_date", "end_date" }
        for key in d.keys():
            if key not in attrs:
                raise ValueError(f"Improper key provided in attribute mapping for Consumable: {key}")

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

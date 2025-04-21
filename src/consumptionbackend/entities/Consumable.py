# stdlib
from dataclasses import dataclass
from datetime import datetime

# consumption
from .EntityBase import EntityBase
from .Status import Status


@dataclass
class Consumable(EntityBase):
    series_id: int
    name: str
    type: str
    status: Status
    parts: int
    max_parts: int | None
    completions: int
    rating: float | None
    start_date: datetime
    end_date: datetime

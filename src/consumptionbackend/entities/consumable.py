from dataclasses import dataclass
from datetime import datetime

from .base import EntityBase
from .status import Status
from .types import Id


@dataclass
class Consumable(EntityBase):
    series_id: Id
    name: str
    type: str
    status: Status
    parts: int
    max_parts: int | None
    completions: int
    rating: float | None
    start_date: datetime | None
    end_date: datetime | None

    def __post_init__(self):
        self.status = Status(self.status)
        if isinstance(self.start_date, (float, int)):
            self.start_date = datetime.fromtimestamp(self.start_date)
        if isinstance(self.end_date, (float, int)):
            self.end_date = datetime.fromtimestamp(self.end_date)

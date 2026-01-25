from enum import IntEnum
from typing import override


class Status(IntEnum):
    PLANNING = 0
    IN_PROGRESS = 1
    ON_HOLD = 2
    DROPPED = 3
    COMPLETED = 4

    @override
    def __str__(self) -> str:
        return self.name.replace("_", " ")

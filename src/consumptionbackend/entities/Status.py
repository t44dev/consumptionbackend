from enum import IntEnum


class Status(IntEnum):
    PLANNING = 0
    IN_PROGRESS = 1
    ON_HOLD = 2
    DROPPED = 3
    COMPLETED = 4

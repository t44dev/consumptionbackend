# stdlib
from typing import final

# consumption
from consumptionbackend.database.database_handling import (
    DatabaseHandlerBase,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from consumptionbackend.database import ConsumableHandlerBase


@final
class SQLiteConsumableHandlerBase(ConsumableHandlerBase):

    _HANDLER: type[DatabaseHandlerBase] = SQLiteDatabaseHandler

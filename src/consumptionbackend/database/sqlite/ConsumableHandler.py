# stdlib

# consumption
from consumptionbackend.database.database_handling import (
    DatabaseHandlerBase,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from consumptionbackend.database import ConsumableHandlerBase


class SQLiteConsumableHandlerBase(ConsumableHandlerBase):

    HANDLER: type[DatabaseHandlerBase] = SQLiteDatabaseHandler

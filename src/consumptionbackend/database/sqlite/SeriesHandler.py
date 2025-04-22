# stdlib

# consumption
from consumptionbackend.database.database_handling import (
    DatabaseHandlerBase,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from consumptionbackend.database import SeriesHandlerBase


class SQLiteSeriesHandlerBase(SeriesHandlerBase):

    HANDLER: type[DatabaseHandlerBase] = SQLiteDatabaseHandler

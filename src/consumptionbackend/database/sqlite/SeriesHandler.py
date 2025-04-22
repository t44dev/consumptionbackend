# stdlib
from typing import final

# consumption
from consumptionbackend.database.database_handling import (
    DatabaseHandlerBase,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from consumptionbackend.database import SeriesHandlerBase


@final
class SQLiteSeriesHandlerBase(SeriesHandlerBase):

    _HANDLER: type[DatabaseHandlerBase] = SQLiteDatabaseHandler

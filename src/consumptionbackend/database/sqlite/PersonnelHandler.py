# stdlib

# consumption
from consumptionbackend.database.database_handling import (
    DatabaseHandlerBase,
)
from .SQLiteDatabaseHandler import SQLiteDatabaseHandler
from consumptionbackend.database import PersonnelHandlerBase


class SQLitePersonnelHandlerBase(PersonnelHandlerBase):

    HANDLER: type[DatabaseHandlerBase] = SQLiteDatabaseHandler

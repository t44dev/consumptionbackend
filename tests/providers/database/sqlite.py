# stdlib
import sqlite3
from typing import override

# consumption
from consumptionbackend.config import ConsumptionConfig
from consumptionbackend.database.sqlite.database_provider import (
    SQLiteFileDatabaseProvider,
)


class SQLiteMemoryDatabaseProvider(SQLiteFileDatabaseProvider):

    @override
    @classmethod
    def setup(cls) -> sqlite3.Connection:
        config = ConsumptionConfig()
        conn = sqlite3.connect(":memory:")
        cls.migrate(conn, None, config.CURRENT_VERSION)
        return conn

    @override
    @classmethod
    def reset(cls) -> None:
        SQLiteMemoryDatabaseProvider().db.close()
        super().reset()

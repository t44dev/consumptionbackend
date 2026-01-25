import sqlite3
from pathlib import Path
from typing import override

from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine


class SQLiteMemoryDatabaseEngine(SQLiteDatabaseEngine):
    def __init__(self) -> None:
        super().__init__(":memory:")

    @override
    @classmethod
    def setup(cls, db_path: Path | str) -> sqlite3.Connection:
        db = cls.connect(db_path)
        cls.migrate(db, None)

        return db

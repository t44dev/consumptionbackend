import sqlite3
from abc import ABC, abstractmethod
from glob import glob
from importlib import resources
from pathlib import Path
from typing import override

from consumptionbackend.utils import ServiceBase


class SQLiteDatabaseEngine(ServiceBase, ABC):
    def __init__(self, db_path: Path | str) -> None:
        self.db: sqlite3.Connection = self.setup(db_path)

    @classmethod
    @abstractmethod
    def setup(
        cls,
        db_path: Path | str,
    ) -> sqlite3.Connection: ...

    @classmethod
    def connect(cls, db_path: Path | str) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _ = conn.execute("PRAGMA foreign_keys = ON").fetchall()

        return conn

    @classmethod
    def migrate(cls, db: sqlite3.Connection, version: str | None) -> None:
        # TODO: Can this be made dynamic?
        migrations_dir = resources.files(
            "consumptionbackend.database.sqlite.migrations"
        )
        files = [
            file
            for file in sorted(glob(str(migrations_dir / "v[0-9].[0-9].[0-9].sql")))
            if version is None or file > version
        ]

        if len(files) == 0:
            return

        cur = db.cursor()
        for file in files:
            script_content = Path(file).read_text()
            _ = cur.executescript(script_content)

        cur.close()


class SQLiteFileDatabaseEngine(SQLiteDatabaseEngine):
    def __init__(self, db_path: Path | str) -> None:
        super().__init__(db_path)

    @override
    @classmethod
    def setup(cls, db_path: Path | str) -> sqlite3.Connection:
        if isinstance(db_path, str):
            db_path = Path(db_path)

        if not db_path.is_file():
            db_path.parent.mkdir(exist_ok=True, parents=True)
            db = cls.connect(db_path)
            version = None
        else:
            db = cls.connect(db_path)
            cur = db.cursor()
            _ = cur.execute("SELECT major, minor, patch from version")
            major, minor, patch = cur.fetchone()
            cur.close()
            version = f"{major}.{minor}.{patch}"

        cls.migrate(db, version)

        return db

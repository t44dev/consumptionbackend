# stdlib
from glob import glob
from importlib import resources
from pathlib import Path
import sqlite3

# consumption
from consumptionbackend.config.config import ConsumptionConfig
from consumptionbackend.database.database_provider import DatabaseProviderBase


class SQLiteFileDatabaseProvider(DatabaseProviderBase):

    @classmethod
    def setup(cls) -> sqlite3.Connection:
        config = ConsumptionConfig()
        db_path = Path(config["db"])

        if not db_path.is_file():
            db_path.parent.mkdir(exist_ok=True, parents=True)
            conn = sqlite3.connect(db_path)
            cls.migrate(conn, None, config.CURRENT_VERSION)
            return conn

        conn = sqlite3.connect(db_path)
        version = config["version"]
        if version != config.CURRENT_VERSION:
            cls.migrate(conn, version, config.CURRENT_VERSION)
            config["version"] = config.CURRENT_VERSION
            config.write()

        return conn

    @classmethod
    def migrate(
        cls, conn: sqlite3.Connection, version_start: str | None, version_end: str
    ) -> None:
        cur = conn.cursor()

        # TODO: Can this be made dynamic?
        migrations_dir = resources.files(
            "consumptionbackend.database.sqlite.migrations"
        )
        files = filter(
            lambda x: (version_start is None or x > version_start) and x <= version_end,
            sorted(glob(str(migrations_dir / "v[0-9].[0-9].[0-9].sql"))),
        )
        for file in files:
            script_content = Path(file).read_text()
            _ = cur.executescript(script_content)

        cur.close()

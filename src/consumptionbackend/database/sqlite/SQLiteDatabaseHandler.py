# stdlib
from collections.abc import Mapping, Sequence
from glob import glob
from importlib import resources
from pathlib import Path
import sqlite3
from typing import Any, TypeVar, Unpack, final

# consumption
from consumptionbackend.config.config import ConsumptionConfig
from consumptionbackend.database.base_handlers import DatabaseHandlerBase, WhereMapping
from consumptionbackend.database.queries import ApplyQuery
from consumptionbackend.entities import EntityBase

E = TypeVar("E", bound=EntityBase)


@final
class SQLiteDatabaseHandler(DatabaseHandlerBase):

    def __init__(self) -> None:
        super().__init__()
        self.db: sqlite3.Connection = SQLiteDatabaseHandler.setup()

    def new(self, t: type[E], **values: Mapping[str, Any]) -> E:
        pass

    def find_by_id(self, t: type[E], id: int) -> E:
        pass

    def find(self, t: type[E], **where: Unpack[WhereMapping]) -> Sequence[E]:
        pass

    def update(
        self,
        t: type[E],
        where: WhereMapping,
        apply: Mapping[str, ApplyQuery[Any]],
    ) -> Sequence[E]:
        pass

    def delete(self, t: type[E], **where: Unpack[WhereMapping]) -> None:
        pass

    @classmethod
    def setup(cls) -> sqlite3.Connection:
        config = ConsumptionConfig()
        db_path = Path(config["db"])

        if not db_path.is_file():
            db_path.parent.mkdir(exist_ok=True, parents=True)
            conn = sqlite3.connect(db_path)
            SQLiteDatabaseHandler.migrate(conn, None, config.CURRENT_VERSION)
            return conn

        conn = sqlite3.connect(db_path)
        version = config["version"]
        if version != config.CURRENT_VERSION:
            SQLiteDatabaseHandler.migrate(conn, version, config.CURRENT_VERSION)

        return conn

    @classmethod
    def migrate(
        cls, conn: sqlite3.Connection, version_start: str | None, version_end: str
    ) -> None:

        # TODO: Can this be made dynamic?
        migrations_dir = resources.files(
            "consumptionbackend.database.sqlite.migrations"
        )
        files = filter(
            lambda x: (version_start is None or x > version_start) and x <= version_end,
            sorted(glob(str(migrations_dir / "v[0-9].[0-9].[0-9].sql"))),
        )
        for file in files:
            _ = conn.executescript(file)

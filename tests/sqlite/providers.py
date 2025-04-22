# stdlib
import sqlite3
from typing import final

# consumption
from consumptionbackend.config.config import ConsumptionConfig
from consumptionbackend.config.config_provider import ConfigDict, ConfigProvider
from consumptionbackend.database.sqlite.database_provider import (
    SQLiteFileDatabaseProvider,
)


@final
class MemoryConfigProvider(ConfigProvider):

    def __init__(self) -> None:
        self.config: ConfigDict = ConsumptionConfig.DEFAULT_CONFIG

    def setup(self) -> ConfigDict:
        return self.config

    def read(self) -> ConfigDict:
        return self.config

    def write(self, config: ConfigDict) -> None:
        self.config = config


class SQLiteMemoryDatabaseProvider(SQLiteFileDatabaseProvider):

    @classmethod
    def setup(cls) -> sqlite3.Connection:
        config = ConsumptionConfig()
        conn = sqlite3.connect(":memory:")
        cls.migrate(conn, None, config.CURRENT_VERSION)
        return conn

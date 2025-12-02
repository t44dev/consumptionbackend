# stdlib
import unittest
from collections.abc import Mapping, Sequence
from typing import override, Any

# consumption
from consumptionbackend.config import ConsumptionConfig
from consumptionbackend.database.sqlite import SQLiteDatabaseHandler
from consumptionbackend.database.sqlite.sql_utils import fix_value
from tests.providers.database.sqlite import SQLiteMemoryDatabaseProvider
from tests.providers.config import MemoryConfigProvider


class SQLiteUnitTestBase(unittest.TestCase):
    @override
    @classmethod
    def setUpClass(cls):
        ConsumptionConfig._PROVIDER = (  # pyright:ignore[reportPrivateUsage]
            MemoryConfigProvider()
        )
        _ = ConsumptionConfig()
        SQLiteDatabaseHandler.PROVIDER = SQLiteMemoryDatabaseProvider
        _ = SQLiteDatabaseHandler()
        return super().setUpClass()

    @override
    @classmethod
    def tearDownClass(cls):
        ConsumptionConfig.reset()
        return super().tearDownClass()

    @classmethod
    def normalise_sql(cls, sql: str) -> str:
        return " ".join(sql.split())

    @classmethod
    def normalise_values(cls, values: Mapping[str, Any]) -> Sequence[Any]:
        return [fix_value(v) for v in values.values()]

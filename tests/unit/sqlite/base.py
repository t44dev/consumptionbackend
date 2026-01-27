import unittest
from collections.abc import Mapping, Sequence
from typing import Any, override

from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.database.sqlite.sql_utils import fix_value
from consumptionbackend.utils import ServiceProvider
from tests.services.database.sqlite import SQLiteMemoryDatabaseEngine


class SQLiteUnitTestBase(unittest.TestCase):
    @override
    @classmethod
    def setUpClass(cls):
        ServiceProvider.register(SQLiteDatabaseEngine, SQLiteMemoryDatabaseEngine())
        return super().setUpClass()

    @classmethod
    def normalise_sql(cls, sql: str) -> str:
        return " ".join(sql.split())

    @classmethod
    def normalise_values(cls, values: Mapping[str, Any]) -> Sequence[Any]:
        return [fix_value(v) for v in values.values()]

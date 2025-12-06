import unittest
from collections.abc import Sequence
from typing import Callable, TypeVar, override

from consumptionbackend.config import ConsumptionConfig
from consumptionbackend.database.sqlite import SQLiteDatabaseHandler
from tests.providers.config import MemoryConfigProvider
from tests.providers.database.sqlite import SQLiteMemoryDatabaseProvider

T = TypeVar("T")


class SQLiteIntegrationTestBase(unittest.TestCase):
    @override
    @classmethod
    def setUpClass(cls):
        ConsumptionConfig._PROVIDER = (  # pyright:ignore[reportPrivateUsage]
            MemoryConfigProvider()
        )
        SQLiteDatabaseHandler.PROVIDER = SQLiteMemoryDatabaseProvider
        return super().setUpClass()

    @override
    def tearDown(self):
        ConsumptionConfig.reset()
        SQLiteMemoryDatabaseProvider.reset()
        return super().tearDownClass()

    def assertSingle(
        self, seq: Sequence[T], predicate: Callable[[T], bool] = lambda _: True
    ) -> None:
        matches = [x for x in seq if predicate(x)]
        if len(matches) != 1:
            self.fail(f"Expected single but found {len(matches)}")

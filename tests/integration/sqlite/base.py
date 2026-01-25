import unittest
from collections.abc import Sequence
from typing import Callable, override

from consumptionbackend.database import (
    ConsumableService,
    PersonnelService,
    SeriesService,
)
from consumptionbackend.database.sqlite import (
    SQLiteConsumableService,
    SQLitePersonnelService,
    SQLiteSeriesService,
)
from consumptionbackend.database.sqlite.engine import SQLiteDatabaseEngine
from consumptionbackend.utils import ServiceProvider
from tests.services.database.sqlite import SQLiteMemoryDatabaseEngine


class SQLiteIntegrationTestBase(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.sqlite_engine: SQLiteDatabaseEngine | None = None

    @override
    @classmethod
    def setUpClass(cls):
        ServiceProvider.register(ConsumableService, SQLiteConsumableService())
        ServiceProvider.register(SeriesService, SQLiteSeriesService())
        ServiceProvider.register(PersonnelService, SQLitePersonnelService())
        return super().setUpClass()

    @override
    def setUp(self) -> None:
        self.sqlite_engine = SQLiteMemoryDatabaseEngine()
        ServiceProvider.register(SQLiteDatabaseEngine, self.sqlite_engine)
        return super().setUp()

    @override
    def tearDown(self):
        if self.sqlite_engine is not None:
            self.sqlite_engine.db.close()
        return super().tearDownClass()

    def assertSingle[T](
        self, seq: Sequence[T], predicate: Callable[[T], bool] = lambda _: True
    ) -> None:
        matches = [x for x in seq if predicate(x)]
        if len(matches) != 1:
            self.fail(f"Expected single but found {len(matches)}")

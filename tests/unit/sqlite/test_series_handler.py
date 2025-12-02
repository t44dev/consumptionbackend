# stdlib
import unittest

# consumption
from consumptionbackend.database.sqlite.SeriesHandler import SQLiteSeriesHandler
from .base import SQLiteUnitTestBase


class TestSQLiteSeriesHandler(SQLiteUnitTestBase):
    def test_consumables_simple(self):
        series_id = 44
        expected_sql = "SELECT * FROM consumables WHERE series_id = ?"

        (sql, values) = (
            SQLiteSeriesHandler._consumables_sql(  # pyright: ignore[reportPrivateUsage]
                series_id
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [series_id])


if __name__ == "__main__":
    _ = unittest.main()

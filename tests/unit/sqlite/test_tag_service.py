import unittest

from consumptionbackend.database.sqlite import SQLiteTagService

from .base import SQLiteUnitTestBase


class TestSQLiteTagService(SQLiteUnitTestBase):
    def test_find(self):
        expected_sql = "SELECT DISTINCT tag FROM consumable_tags ct"

        (sql, values) = SQLiteTagService()._find_sql()  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [])


if __name__ == "__main__":
    _ = unittest.main()

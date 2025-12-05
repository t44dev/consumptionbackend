import unittest

from consumptionbackend.database.sqlite.PersonnelHandler import SQLitePersonnelHandler

from .base import SQLiteUnitTestBase


class TestSQLitePersonnelHandler(SQLiteUnitTestBase):
    def test_consumables_simple(self):
        personnel_id = 44
        expected_sql = "SELECT consumable_id as id, role FROM consumable_personnel WHERE personnel_id = ?"

        (sql, values) = SQLitePersonnelHandler._consumables_sql(  # pyright: ignore[reportPrivateUsage]
            personnel_id
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [personnel_id])


if __name__ == "__main__":
    _ = unittest.main()

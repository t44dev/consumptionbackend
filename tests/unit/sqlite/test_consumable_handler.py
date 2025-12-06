import unittest

from consumptionbackend.database.sqlite.ConsumableHandler import SQLiteConsumableHandler
from tests.test_data import COMPLEX_WHERE, COMPLEX_WHERE_VALUES

from .base import SQLiteUnitTestBase


class TestSQLiteConsumableHandler(SQLiteUnitTestBase):
    def test_series(self):
        consumable_id = 44
        expected_sql = "SELECT * FROM series t1 WHERE t1.id = ( SELECT t2.series_id FROM consumables t2 WHERE t2.id = ? )"

        (sql, values) = SQLiteConsumableHandler._series_sql(  # pyright: ignore[reportPrivateUsage]
            consumable_id
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [consumable_id])

    def test_personnel(self):
        consumable_id = 44_444
        expected_sql = "SELECT personnel_id as id, role FROM consumable_personnel WHERE consumable_id = ?"

        (sql, values) = SQLiteConsumableHandler._personnel_sql(  # pyright: ignore[reportPrivateUsage]
            consumable_id
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [consumable_id])

    def test_tags(self):
        consumable_id = 444_444
        expected_sql = "SELECT tag FROM consumable_tags WHERE consumable_id = ?"

        (sql, values) = SQLiteConsumableHandler._tags_sql(  # pyright: ignore[reportPrivateUsage]
            consumable_id
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [consumable_id])

    def test_add_personnel(self):
        role = "AddRole"
        expected_sql = (
            "INSERT OR IGNORE INTO consumable_personnel (consumable_id, personnel_id, role) SELECT * FROM ( SELECT c.id as consumable_id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) CROSS JOIN ( SELECT p.id as personnel_id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) CROSS JOIN ( SELECT ? as role )"
        )

        (sql, values) = SQLiteConsumableHandler._add_personnel_sql(  # pyright: ignore[reportPrivateUsage]
            COMPLEX_WHERE, COMPLEX_WHERE, role
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(
            values, [*COMPLEX_WHERE_VALUES, *COMPLEX_WHERE_VALUES, role]
        )

    def test_remove_personnel(self):
        roles = {"role1", "role2", "role3"}
        expected_sql = (
            "DELETE FROM consumable_personnel WHERE consumable_id IN ( SELECT c.id as consumable_id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) AND personnel_id IN ( SELECT p.id as personnel_id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) AND role IN (?, ?, ?)"
        )

        (sql, values) = SQLiteConsumableHandler._remove_personnel_sql(  # pyright: ignore[reportPrivateUsage]
            COMPLEX_WHERE, COMPLEX_WHERE, roles
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(
            values, [*COMPLEX_WHERE_VALUES, *COMPLEX_WHERE_VALUES, *roles]
        )

    def test_add_tags(self):
        ids = [4, 44, 444, 4_444]
        tags = ["tag1", "tag2", "tag3"]
        expected_sql = "INSERT OR IGNORE INTO consumable_tags (consumable_id, tag) SELECT * FROM (VALUES (?), (?), (?), (?)) CROSS JOIN (VALUES (?), (?), (?))"

        (sql, values) = SQLiteConsumableHandler._add_tags_sql(  # pyright: ignore[reportPrivateUsage]
            ids, tags
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [*ids, *tags])

    def test_remove_tags_simple(self):
        ids = [4, 44, 444, 4_444]
        tags = ["tag1", "tag2", "tag3"]
        expected_sql = "DELETE FROM consumable_tags WHERE consumable_id IN (?, ?, ?, ?) AND tag IN (?, ?, ?)"

        (sql, values) = SQLiteConsumableHandler._remove_tags_sql(  # pyright: ignore[reportPrivateUsage]
            ids, tags
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [*ids, *tags])


if __name__ == "__main__":
    _ = unittest.main()

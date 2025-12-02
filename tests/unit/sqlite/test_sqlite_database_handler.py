# stdlib
import unittest

# consumption
from consumptionbackend.database import (
    ApplyOperator,
    ApplyQuery,
)
from consumptionbackend.entities import (
    Consumable,
    Personnel,
    Series,
)
from consumptionbackend.database.sqlite import SQLiteDatabaseHandler
from tests.unit.sqlite.base import SQLiteUnitTestBase
from tests.test_data import (
    COMPLEX_WHERE,
    COMPLEX_WHERE_VALUES,
    CONSUMABLE_REQUIRED,
    PERSONNEL_REQUIRED,
    SERIES_REQUIRED,
    SIMPLE_WHERE,
    SIMPLE_WHERE_VALUES,
)


class TestSQLiteDatabaseHandler(SQLiteUnitTestBase):

    def test_new(self):
        cases = {
            Consumable: (
                CONSUMABLE_REQUIRED,
                "INSERT INTO consumables (series_id, name, type, status, parts, max_parts, completions, rating, start_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ),
            Personnel: (
                PERSONNEL_REQUIRED,
                "INSERT INTO personnel (first_name, last_name, pseudonym) VALUES (?, ?, ?)",
            ),
            Series: (SERIES_REQUIRED, "INSERT INTO series (name) VALUES (?)"),
        }

        for entity, (fields, expected_sql) in cases.items():
            with self.subTest(entity=entity, fields=fields, expected_sql=expected_sql):
                (sql, values) = (
                    SQLiteDatabaseHandler._new_sql(  # pyright: ignore[reportPrivateUsage]
                        entity, **fields
                    )
                )

                self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
                self.assertSequenceEqual(
                    values, SQLiteUnitTestBase.normalise_values(fields)
                )

    def test_find_by_id(self):
        cases = {
            Consumable: (5, "SELECT * FROM consumables WHERE id = ?"),
            Personnel: (2, "SELECT * FROM personnel WHERE id = ?"),
            Series: (999_999, "SELECT * FROM series WHERE id = ?"),
        }

        for entity, (id, expected_sql) in cases.items():
            (sql, value) = (
                SQLiteDatabaseHandler._find_by_id_sql(  # pyright: ignore[reportPrivateUsage]
                    entity, id
                )
            )

            self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
            self.assertSequenceEqual(value, [id])

    def test_find_by_ids(self):
        cases = {
            Consumable: ([5], "SELECT * FROM consumables WHERE id IN (?)"),
            Personnel: (
                [2, 9, 6, 3, 8],
                "SELECT * FROM personnel WHERE id IN (?, ?, ?, ?, ?)",
            ),
            Series: (
                [100, 34, 1, 999_999],
                "SELECT * FROM series WHERE id IN (?, ?, ?, ?)",
            ),
        }

        for entity, (ids, expected_sql) in cases.items():
            (sql, values) = (
                SQLiteDatabaseHandler._find_by_ids_sql(  # pyright: ignore[reportPrivateUsage]
                    entity, ids
                )
            )

            self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
            self.assertSequenceEqual(values, ids)

    def test_find_simple(self):
        expected_sql = (
            "SELECT DISTINCT c.* FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE LOWER(c.name) LIKE ? AND c.completions > ?"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._find_sql(  # pyright: ignore[reportPrivateUsage]
                Consumable, **SIMPLE_WHERE
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, SIMPLE_WHERE_VALUES)

    def test_find_complex(self):
        expected_sql = (
            "SELECT DISTINCT p.* FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._find_sql(  # pyright: ignore[reportPrivateUsage]
                Personnel, **COMPLEX_WHERE
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, COMPLEX_WHERE_VALUES)

    def test_find_no_where(self):
        expected_sql = (
            "SELECT DISTINCT s.* "
            "FROM consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._find_sql(  # pyright: ignore[reportPrivateUsage]
                Series
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [])

    def test_update_simple(self):
        apply = {"name": ApplyQuery("ApplyName", ApplyOperator.APPLY)}
        expected_sql = (
            "UPDATE consumables SET name = ? WHERE id IN ( SELECT c.id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE LOWER(c.name) LIKE ? AND c.completions > ?"
            " ) RETURNING id"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._update_sql(  # pyright: ignore[reportPrivateUsage]
                Consumable, SIMPLE_WHERE, apply
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, ["ApplyName", *SIMPLE_WHERE_VALUES])

    def test_update_complex(self):
        apply = {
            "first_name": ApplyQuery("ApplyFirstName", ApplyOperator.APPLY),
            "last_name": ApplyQuery(None, ApplyOperator.APPLY),
            "pseudonym": ApplyQuery("ApplyLastName", ApplyOperator.APPLY),
        }
        apply_values = ["ApplyFirstName", None, "ApplyLastName"]
        expected_sql = (
            "UPDATE personnel SET first_name = ?, last_name = ?, pseudonym = ? WHERE id IN ( SELECT p.id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) RETURNING id"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._update_sql(  # pyright: ignore[reportPrivateUsage]
                Personnel, COMPLEX_WHERE, apply
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, [*apply_values, *COMPLEX_WHERE_VALUES])

    def test_delete_simple(self):
        expected_sql = (
            "DELETE FROM series WHERE id IN ( SELECT s.id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE LOWER(c.name) LIKE ? AND c.completions > ?"
            " ) RETURNING id"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._delete_sql(  # pyright: ignore[reportPrivateUsage]
                Series, **SIMPLE_WHERE
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, SIMPLE_WHERE_VALUES)

    def test_delete_complex(self):
        expected_sql = (
            "DELETE FROM consumables WHERE id IN ( SELECT c.id FROM "
            "consumables c FULL OUTER JOIN series s ON s.id = c.series_id FULL OUTER JOIN consumable_personnel cp ON cp.consumable_id = c.id FULL OUTER JOIN personnel p ON p.id = cp.personnel_id"
            " WHERE c.id > ? AND c.id <= ? AND c.series_id >= ? AND c.series_id < ? AND LOWER(c.name) LIKE ? AND c.type = ? AND c.status >= ? AND c.status < ? AND c.parts > ? AND c.parts < ? AND c.max_parts > ? AND c.max_parts < ? AND c.completions > ? AND c.completions < ? AND c.rating > ? AND c.rating > ? AND c.rating > ? AND c.start_date > ? AND c.start_date < ? AND c.end_date = ? AND p.id > ? AND p.id <= ? AND LOWER(p.first_name) LIKE ? AND p.last_name = ? AND LOWER(p.pseudonym) LIKE ? AND cp.role = ? AND s.id > ? AND s.id <= ? AND LOWER(s.name) LIKE ?"
            " ) RETURNING id"
        )

        (sql, values) = (
            SQLiteDatabaseHandler._delete_sql(  # pyright: ignore[reportPrivateUsage]
                Consumable, **COMPLEX_WHERE
            )
        )

        self.assertEqual(SQLiteUnitTestBase.normalise_sql(sql), expected_sql)
        self.assertSequenceEqual(values, COMPLEX_WHERE_VALUES)


if __name__ == "__main__":
    _ = unittest.main()

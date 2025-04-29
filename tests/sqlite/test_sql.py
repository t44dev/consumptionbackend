# stdlib
import datetime
import unittest

# consumption
from consumptionbackend.config import ConsumptionConfig
from consumptionbackend.database import *
from consumptionbackend.entities import *
from consumptionbackend.database.sqlite.SQLiteDatabaseHandler import (
    SQLiteDatabaseHandler,
)
from tests.sqlite.providers import SQLiteMemoryDatabaseProvider, MemoryConfigProvider


class TestSQL(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ConsumptionConfig._PROVIDER = (  # pyright:ignore[reportPrivateUsage]
            MemoryConfigProvider()
        )
        _ = ConsumptionConfig()
        SQLiteDatabaseHandler.PROVIDER = SQLiteMemoryDatabaseProvider
        _ = SQLiteDatabaseHandler()
        return super().setUpClass()

    def test_new(self) -> None:
        dt = datetime.datetime.fromtimestamp(9999)
        values: ConsumableFieldsRequired = {
            "series_id": 2,
            "name": "1984",
            "type": "NOVEL",
            "status": Status.ON_HOLD,
            "parts": 3,
            "max_parts": 9,
            "completions": 2,
            "rating": 9.8,
            "start_date": dt,
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._new_sql(  # pyright:ignore[reportPrivateUsage]
            Consumable, **values
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "INSERT INTO consumables (series_id, name, type, status, parts, max_parts, completions, rating, start_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        expected_values = [
            2,
            "1984",
            "NOVEL",
            2,
            3,
            9,
            2,
            9.8,
            dt.timestamp(),
        ]

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, expected_values)

    def test_simple_find_by_id(self) -> None:
        id = 5

        (
            sql,
            value,
        ) = SQLiteDatabaseHandler()._find_by_id_sql(  # pyright:ignore[reportPrivateUsage]
            Series, id
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "SELECT * FROM series WHERE id = ?"

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(value, [id])

    def test_simple_find(self) -> None:
        values: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("John Ronald Reuel", WhereOperator.LIKE)],
                "role": [WhereQuery("Author", WhereOperator.EQ)],
            }
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._find_sql(  # pyright:ignore[reportPrivateUsage]
            Personnel, **values
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "SELECT p.* FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE LOWER(p.first_name) LIKE ? AND cp.role = ?"

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, ["%john ronald reuel%", "Author"])

    def test_complex_find(self) -> None:
        dtgt = datetime.datetime.fromtimestamp(9898)
        dtlte = datetime.datetime.fromtimestamp(9999)
        values: WhereMapping = {
            "consumables": {
                "status": [WhereQuery(Status.COMPLETED, WhereOperator.EQ)],
                "end_date": [
                    WhereQuery(dtgt, WhereOperator.GT),
                    WhereQuery(dtlte, WhereOperator.LTE),
                ],
            },
            "series": {"name": [WhereQuery("Lord of the Rings", WhereOperator.EQ)]},
            "personnel": {
                "first_name": [WhereQuery("John Ronald Reuel", WhereOperator.LIKE)],
                "role": [WhereQuery("Author", WhereOperator.EQ)],
            },
            "consumable_tags": {
                "tag": [
                    WhereQuery("fun", WhereOperator.EQ),
                    WhereQuery("cool", WhereOperator.EQ),
                    WhereQuery("bad", WhereOperator.NEQ),
                    WhereQuery("awesome", WhereOperator.EQ),
                    WhereQuery("weak", WhereOperator.NEQ),
                    WhereQuery("yawn", WhereOperator.NEQ),
                ]
            },
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._find_sql(  # pyright:ignore[reportPrivateUsage]
            Personnel, **values
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "SELECT p.* FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE c.status = ? AND c.end_date > ? AND c.end_date <= ? AND s.name = ? AND LOWER(p.first_name) LIKE ? AND cp.role = ? AND c.id IN ( SELECT tgw.consumable_id FROM consumable_tags tgw WHERE tgw.tag IN (? ? ?) AND tgw.tag NOT IN (? ? ?) )"
        expected_values = [
            4,
            dtgt.timestamp(),
            dtlte.timestamp(),
            "Lord of the Rings",
            "%john ronald reuel%",
            "Author",
            "fun",
            "cool",
            "awesome",
            "bad",
            "weak",
            "yawn",
        ]

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, expected_values)

    def test_simple_update(self) -> None:
        where: WhereMapping = {
            "series": {"name": [WhereQuery("Lort of the", WhereOperator.LIKE)]}
        }
        apply: SeriesApplyMapping = {
            "name": ApplyQuery("Lord of the Rings", ApplyOperator.APPLY)
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._update_sql(  # pyright:ignore[reportPrivateUsage]
            Series, where, apply
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "UPDATE series SET name = ? WHERE id IN ( SELECT s.id FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE LOWER(s.name) LIKE ? ) RETURNING *"
        expected_values = ["Lord of the Rings", "%lort of the%"]

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, expected_values)

    def test_complex_update(self) -> None:
        dtgt = datetime.datetime.fromtimestamp(9898)
        dtlte = datetime.datetime.fromtimestamp(9999)
        dtsub = datetime.datetime.fromtimestamp(1000)
        where: WhereMapping = {
            "consumables": {
                "status": [WhereQuery(Status.COMPLETED, WhereOperator.EQ)],
                "end_date": [
                    WhereQuery(dtgt, WhereOperator.GT),
                    WhereQuery(dtlte, WhereOperator.LTE),
                ],
            },
            "series": {"name": [WhereQuery("Lord of the Rings", WhereOperator.EQ)]},
            "personnel": {
                "first_name": [WhereQuery("John Ronald Reuel", WhereOperator.LIKE)],
                "role": [WhereQuery("Author", WhereOperator.EQ)],
            },
            "consumable_tags": {
                "tag": [
                    WhereQuery("fun", WhereOperator.EQ),
                    WhereQuery("cool", WhereOperator.EQ),
                    WhereQuery("bad", WhereOperator.NEQ),
                    WhereQuery("awesome", WhereOperator.EQ),
                    WhereQuery("weak", WhereOperator.NEQ),
                    WhereQuery("yawn", WhereOperator.NEQ),
                ]
            },
        }
        apply: ConsumableApplyMapping = {
            "name": ApplyQuery("Lord of The Rings", ApplyOperator.APPLY),
            "type": ApplyQuery("TV", ApplyOperator.APPLY),
            "parts": ApplyQuery(3, ApplyOperator.ADD),
            "end_date": ApplyQuery(dtsub, ApplyOperator.SUB),
            "series_id": ApplyQuery(5, ApplyOperator.APPLY),
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._update_sql(  # pyright:ignore[reportPrivateUsage]
            Consumable, where, apply
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "UPDATE consumables SET name = ?, type = ?, parts = parts + ?, end_date = end_date - ?, series_id = ? WHERE id IN ( SELECT c.id FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE c.status = ? AND c.end_date > ? AND c.end_date <= ? AND s.name = ? AND LOWER(p.first_name) LIKE ? AND cp.role = ? AND c.id IN ( SELECT tgw.consumable_id FROM consumable_tags tgw WHERE tgw.tag IN (? ? ?) AND tgw.tag NOT IN (? ? ?) ) ) RETURNING *"
        expected_values = [
            "Lord of The Rings",
            "TV",
            3,
            dtsub.timestamp(),
            5,
            4,
            dtgt.timestamp(),
            dtlte.timestamp(),
            "Lord of the Rings",
            "%john ronald reuel%",
            "Author",
            "fun",
            "cool",
            "awesome",
            "bad",
            "weak",
            "yawn",
        ]

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, expected_values)

    def test_simple_delete(self) -> None:
        values: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("John Ronald Reuel", WhereOperator.LIKE)],
                "role": [WhereQuery("Author", WhereOperator.EQ)],
            }
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._delete_sql(  # pyright:ignore[reportPrivateUsage]
            Personnel, **values
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "DELETE FROM personnel t WHERE t.id IN ( SELECT p.id FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE LOWER(p.first_name) LIKE ? AND cp.role = ? )"

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, ["%john ronald reuel%", "Author"])

    def test_complex_delete(self) -> None:
        dtgt = datetime.datetime.fromtimestamp(9898)
        dtlte = datetime.datetime.fromtimestamp(9999)
        values: WhereMapping = {
            "consumables": {
                "status": [WhereQuery(Status.COMPLETED, WhereOperator.EQ)],
                "end_date": [
                    WhereQuery(dtgt, WhereOperator.GT),
                    WhereQuery(dtlte, WhereOperator.LTE),
                ],
            },
            "series": {"name": [WhereQuery("Lord of the Rings", WhereOperator.EQ)]},
            "personnel": {
                "first_name": [WhereQuery("John Ronald Reuel", WhereOperator.LIKE)],
                "role": [WhereQuery("Author", WhereOperator.EQ)],
            },
            "consumable_tags": {
                "tag": [
                    WhereQuery("fun", WhereOperator.EQ),
                    WhereQuery("cool", WhereOperator.EQ),
                    WhereQuery("bad", WhereOperator.NEQ),
                    WhereQuery("awesome", WhereOperator.EQ),
                    WhereQuery("weak", WhereOperator.NEQ),
                    WhereQuery("yawn", WhereOperator.NEQ),
                ]
            },
        }

        (
            sql,
            new_values,
        ) = SQLiteDatabaseHandler()._delete_sql(  # pyright:ignore[reportPrivateUsage]
            Personnel, **values
        )

        stripped_sql = " ".join(sql.split())
        expected_sql = "DELETE FROM personnel t WHERE t.id IN ( SELECT p.id FROM consumables c JOIN series s ON s.id = c.series_id JOIN consumable_personnel cp ON cp.consumable_id = c.id JOIN personnel p ON p.id = cp.personnel_id WHERE c.status = ? AND c.end_date > ? AND c.end_date <= ? AND s.name = ? AND LOWER(p.first_name) LIKE ? AND cp.role = ? AND c.id IN ( SELECT tgw.consumable_id FROM consumable_tags tgw WHERE tgw.tag IN (? ? ?) AND tgw.tag NOT IN (? ? ?) ) )"
        expected_values = [
            4,
            dtgt.timestamp(),
            dtlte.timestamp(),
            "Lord of the Rings",
            "%john ronald reuel%",
            "Author",
            "fun",
            "cool",
            "awesome",
            "bad",
            "weak",
            "yawn",
        ]

        self.assertEqual(stripped_sql, expected_sql)
        self.assertListEqual(new_values, expected_values)

    @classmethod
    def tearDownClass(cls) -> None:
        x = ConsumptionConfig()
        del x
        x = SQLiteDatabaseHandler()
        del x
        return super().setUpClass()

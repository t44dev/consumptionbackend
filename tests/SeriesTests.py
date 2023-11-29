
from consumptionbackend.Personnel import Personnel
from consumptionbackend.Series import Series
from consumptionbackend.Consumable import Consumable
from consumptionbackend.Database import DatabaseHandler, DatabaseInstantiator
import sqlite3
import unittest

db = sqlite3.connect("testdb.db")
DatabaseHandler.DB_CONNECTION = db


class TestSeries(unittest.TestCase):

    def setUp(self) -> None:
        DatabaseInstantiator.run()

    def tearDown(self) -> None:
        db = sqlite3.connect("testdb.db")
        db.cursor().execute(f"DROP TABLE IF EXISTS {Consumable.DB_NAME}")
        db.cursor().execute(
            f"DROP TABLE IF EXISTS {Consumable.DB_PERSONNEL_MAPPING_NAME}")
        db.cursor().execute(f"DROP TABLE IF EXISTS {Series.DB_NAME}")
        db.cursor().execute(f"DROP TABLE IF EXISTS {Personnel.DB_NAME}")

    def test_new(self):
        d = {"name": "test_new"}
        serTest = Series.new(**d)
        serVerify = Series(**d, id=serTest.id)
        self.assertTrue(serTest._precise_eq(serVerify))
        self.assertIsNotNone(serTest.id)

    def test_find(self):
        d = {"name": "test_find"}
        serVerify = Series.new(**d)
        serTest = Series.find(**d)[0]
        self.assertTrue(serTest._precise_eq(serVerify))

    def test_update(self):
        d = {"name": "test_update"}
        serVerify = Series.new(**d)
        where_map = {"name": "test_update"}
        set_map = {"name": "ABC"}
        serTest = Series.update(where_map, set_map)[0]
        serVerify.name = set_map["name"]
        self.assertTrue(serTest._precise_eq(serVerify))

    def test_delete(self):
        d = {"name": "test_delete"}
        Series.new(**d)
        self.assertTrue(Series.delete(name=d["name"]))
        verify = Series.find(name="test_delete")
        self.assertEqual(len(verify), 0)


if __name__ == '__main__':
    unittest.main()

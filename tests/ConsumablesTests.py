from consumptionbackend.Personnel import Personnel
from consumptionbackend.Series import Series
from consumptionbackend.Consumable import Consumable
from consumptionbackend.Database import DatabaseHandler, DatabaseInstantiator
import sqlite3
import unittest

db = sqlite3.connect("testdb.db")
DatabaseHandler.DB_CONNECTION = db


class TestConsumable(unittest.TestCase):

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
        d = {"name": "ABC", "type": "Novel", "status": 1, "parts": 15,
             "completions": 2, "rating": 7.4, "start_date": 1000.5, "end_date": 2000.5}
        consTest = Consumable.new(**d)
        consVerify = Consumable(**d, series_id=-1, id=consTest.id)
        self.assertEqual(consTest, consVerify)
        self.assertIsNotNone(consTest.id)

    def test_find(self):
        d = {"name": "DEF", "type": "Novel", "status": 1, "parts": 15,
             "completions": 2, "rating": 7.4, "start_date": 1000.5, "end_date": 2000.5}
        consVerify = Consumable.new(**d)
        consTest = Consumable.find(**d)[0]
        self.assertEqual(consTest, consVerify)

    def test_update(self):
        d = {"name": "GHI", "type": "Novel", "status": 2, "parts": 15,
             "completions": 2, "rating": 7.4, "start_date": 1000.5, "end_date": 2000.5}
        consVerify = Consumable.new(**d)
        where_map = {"name": "GHI", "parts": 15}
        set_map = {"name": "ABC", "type": "tv"}
        consTest = Consumable.update(where_map, set_map)[0]
        consVerify.name = set_map["name"]
        consVerify.type = set_map["type"].upper()
        self.assertEqual(consTest, consVerify)

    def test_delete(self):
        d = {"name": "JKL", "type": "Novel", "status": 2, "parts": 1337,
             "completions": 2, "rating": 7.4, "start_date": 1000.5, "end_date": 2000.5}
        Consumable.new(**d)
        self.assertTrue(Consumable.delete(parts=d["parts"]))
        verify = Consumable.find(name="JKL")
        self.assertEqual(len(verify), 0)


if __name__ == '__main__':
    unittest.main()

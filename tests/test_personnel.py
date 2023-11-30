from consumptionbackend.Personnel import Personnel
from consumptionbackend.Series import Series
from consumptionbackend.Consumable import Consumable
from consumptionbackend.Database import DatabaseHandler, DatabaseInstantiator
import sqlite3
import unittest

db = sqlite3.connect("testdb.db")
DatabaseHandler.DB_CONNECTION = db


class TestPersonnel(unittest.TestCase):
    def setUp(self) -> None:
        DatabaseInstantiator.run()

    def tearDown(self) -> None:
        db = sqlite3.connect("testdb.db")
        db.cursor().execute(f"DROP TABLE IF EXISTS {Consumable.DB_NAME}")
        db.cursor().execute(
            f"DROP TABLE IF EXISTS {Consumable.DB_PERSONNEL_MAPPING_NAME}"
        )
        db.cursor().execute(f"DROP TABLE IF EXISTS {Series.DB_NAME}")
        db.cursor().execute(f"DROP TABLE IF EXISTS {Personnel.DB_NAME}")

    def test_new(self):
        d = {"first_name": "test_new", "last_name": "World", "pseudonym": "!!"}
        persTest = Personnel.new(**d)
        persVerify = Personnel(**d, id=persTest.id)
        self.assertTrue(persTest._precise_eq(persVerify))
        self.assertIsNotNone(persTest.id)

    def test_find(self):
        d = {"first_name": "test_find", "last_name": "World", "pseudonym": "!!"}
        persVerify = [Personnel.new(**d), Personnel.new(**d)]
        persTest = Personnel.find(**d)
        self.assertEqual(len(persTest), 2)
        self.assertIn(persTest[0], persVerify)

    def test_update(self):
        d = {"first_name": "test_update", "last_name": "World", "pseudonym": "!!"}
        persVerify = Personnel.new(**d)
        where_map = {"first_name": "test_update"}
        set_map = {"pseudonym": "update"}
        persTest = Personnel.update(where_map, set_map)[0]
        persVerify.pseudonym = set_map["pseudonym"]
        self.assertTrue(persTest._precise_eq(persVerify))

    def test_delete(self):
        d = {"first_name": "test_delete", "last_name": "World", "pseudonym": "!!"}
        Personnel.new(**d)
        self.assertTrue(Personnel.delete(first_name=d["first_name"]))
        verify = Personnel.find(**d)
        self.assertEqual(len(verify), 0)


if __name__ == "__main__":
    unittest.main()

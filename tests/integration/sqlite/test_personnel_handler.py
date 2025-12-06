import unittest
from collections.abc import MutableSequence
from datetime import datetime

from consumptionbackend.database import (
    ApplyQuery,
    PersonnelApplyMapping,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.database.sqlite import (
    ConsumableHandler,
    PersonnelHandler,
    SeriesHandler,
)
from consumptionbackend.entities import Id
from consumptionbackend.utils import NotFoundException
from tests.test_data import CONSUMABLE_REQUIRED, PERSONNEL_REQUIRED

from .base import SQLiteIntegrationTestBase


class TestPersonnelIntegration(SQLiteIntegrationTestBase):
    def test_new_simple(self):
        id = PersonnelHandler.new(**PERSONNEL_REQUIRED)
        personnel = PersonnelHandler.find_by_id(id)

        self.assertEqual(personnel.first_name, PERSONNEL_REQUIRED.get("first_name"))
        self.assertEqual(personnel.last_name, PERSONNEL_REQUIRED.get("last_name"))
        self.assertEqual(personnel.pseudonym, PERSONNEL_REQUIRED.get("pseudonym"))

    def test_find_by_id(self):
        id = PersonnelHandler.new(**PERSONNEL_REQUIRED)
        personnel = PersonnelHandler.find_by_id(id)

        self.assertEqual(personnel.first_name, PERSONNEL_REQUIRED.get("first_name"))
        self.assertEqual(personnel.last_name, PERSONNEL_REQUIRED.get("last_name"))
        self.assertEqual(personnel.pseudonym, PERSONNEL_REQUIRED.get("pseudonym"))

    def test_find_by_id_not_found(self):
        id = 44_444

        with self.assertRaises(NotFoundException):
            _ = PersonnelHandler.find_by_id(id)

    def test_find_by_ids(self):
        ids: MutableSequence[Id] = []
        for _ in range(5):
            ids.append(PersonnelHandler.new(**PERSONNEL_REQUIRED))

        found_personnel = PersonnelHandler.find_by_ids(ids)

        for i, personnel in enumerate(found_personnel):
            self.assertEqual(personnel.id, ids[i])

    def test_find_by_ids_not_found(self):
        ids = [44_444, 444_444, 4_444_444]
        found_personnel = PersonnelHandler.find_by_ids(ids)

        self.assertSequenceEqual(found_personnel, [])

    def test_find_simple(self):
        personnel_id1 = PersonnelHandler.new(
            **{
                "first_name": "FoundPersonnel1",
                "last_name": "lIkElAsTnAmE",
                "pseudonym": "Pseudonym",
            }
        )
        personnel_id2 = PersonnelHandler.new(
            **{
                "first_name": "FoundPersonnel2",
                "last_name": "LiKeLaStNaMe",
                "pseudonym": "Pseudonym",
            }
        )
        _ = PersonnelHandler.new(
            **{
                "first_name": "UnfoundPersonnel3",
                "last_name": "LastName",
                "pseudonym": "pseudonym",
            }
        )

        found_personnel = PersonnelHandler.find(
            **{
                "personnel": {
                    "last_name": [WhereQuery("likelastname", WhereOperator.LIKE)],
                    "pseudonym": [WhereQuery("Pseudonym", WhereOperator.EQ)],
                }
            }
        )

        self.assertEqual(len(found_personnel), 2)
        self.assertSingle(
            found_personnel,
            lambda x: x.id == personnel_id1
            and x.first_name == "FoundPersonnel1"
            and x.last_name == "lIkElAsTnAmE"
            and x.pseudonym == "Pseudonym",
        )
        self.assertSingle(
            found_personnel,
            lambda x: x.id == personnel_id2
            and x.first_name == "FoundPersonnel2"
            and x.last_name == "LiKeLaStNaMe"
            and x.pseudonym == "Pseudonym",
        )

    def test_find_complex(self):
        personnel_id1 = PersonnelHandler.new(
            **{**PERSONNEL_REQUIRED, "first_name": "FoundPersonnel1"}
        )
        personnel_id2 = PersonnelHandler.new(
            **{**PERSONNEL_REQUIRED, "first_name": "UnfoundPersonnel2"}
        )

        series_id = SeriesHandler.new(**{"name": "FoundSeries1"})

        consumable_id1 = ConsumableHandler.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": series_id,
                "end_date": datetime.fromtimestamp(44_444),
            }
        )
        consumable_id2 = ConsumableHandler.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": -1,
                "end_date": datetime.fromtimestamp(4_444),
            }
        )

        _ = ConsumableHandler.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id1)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role")],
        )
        _ = ConsumableHandler.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id2)]}},
            {"personnel": {"id": [WhereQuery(personnel_id2)]}},
            [ApplyQuery("Role")],
        )

        complex_where: WhereMapping = {
            "consumables": {
                "end_date": [
                    WhereQuery(datetime.fromtimestamp(4_444), WhereOperator.GT)
                ]
            },
            "personnel": {
                "first_name": [WhereQuery("FoundPersonnel", WhereOperator.LIKE)],
                "role": [WhereQuery("Role", WhereOperator.EQ)],
            },
            "series": {"name": [WhereQuery("FoundSeries1", WhereOperator.EQ)]},
        }

        found_personnel = PersonnelHandler.find(**complex_where)

        self.assertEqual(len(found_personnel), 1)
        personnel = found_personnel[0]
        self.assertEqual(personnel.id, personnel_id1)
        self.assertEqual(personnel.first_name, "FoundPersonnel1")

    def test_find_not_found(self):
        _ = PersonnelHandler.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
            }
        }

        found_personnel = PersonnelHandler.find(**where)

        self.assertEqual(len(found_personnel), 0)

    def test_update(self):
        id = PersonnelHandler.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {"personnel": {"id": [WhereQuery(id)]}}
        apply: PersonnelApplyMapping = {
            "first_name": ApplyQuery("UpdatedFirstName"),
            "pseudonym": ApplyQuery("UpdatedPseudonym"),
        }

        updated_personnel = PersonnelHandler.update(where, apply)

        self.assertEqual(len(updated_personnel), 1)
        personnel = PersonnelHandler.find_by_id(updated_personnel[0])
        self.assertEqual(personnel.id, id)
        self.assertEqual(personnel.first_name, "UpdatedFirstName")
        self.assertEqual(personnel.pseudonym, "UpdatedPseudonym")

    def test_update_not_found(self):
        _ = PersonnelHandler.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
            }
        }
        apply: PersonnelApplyMapping = {"first_name": ApplyQuery("UpdatedFirstName")}

        updated_personnel = PersonnelHandler.update(where, apply)

        self.assertEqual(len(updated_personnel), 0)

    def test_delete(self):
        _ = PersonnelHandler.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "DeletePersonnel1",
            }
        )
        _ = PersonnelHandler.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "DeletePersonnel2",
            }
        )
        _ = PersonnelHandler.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "KeptPersonnel3",
            }
        )

        deleted_personnel_count = PersonnelHandler.delete(
            **{
                "personnel": {
                    "first_name": [WhereQuery("SharedFirstName", WhereOperator.EQ)],
                    "last_name": [WhereQuery("DeletePersonnel", WhereOperator.LIKE)],
                }
            }
        )

        self.assertEqual(deleted_personnel_count, 2)

    def test_delete_not_found(self):
        _ = PersonnelHandler.new(**PERSONNEL_REQUIRED)

        deleted_personnel_count = SeriesHandler.delete(
            **{
                "personnel": {
                    "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
                }
            }
        )

        self.assertEqual(deleted_personnel_count, 0)


if __name__ == "__main__":
    _ = unittest.main()

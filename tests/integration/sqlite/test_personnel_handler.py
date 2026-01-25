import unittest
from collections.abc import MutableSequence
from datetime import datetime

from consumptionbackend.database import (
    ApplyQuery,
    ConsumableService,
    PersonnelApplyMapping,
    PersonnelService,
    SeriesService,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import Id
from consumptionbackend.utils import NotFoundError, ServiceProvider
from tests.test_data import CONSUMABLE_REQUIRED, PERSONNEL_REQUIRED

from .base import SQLiteIntegrationTestBase


class TestPersonnelIntegration(SQLiteIntegrationTestBase):
    def test_new_simple(self):
        service = ServiceProvider.get(PersonnelService)
        id = service.new(**PERSONNEL_REQUIRED)
        personnel = service.find_by_id(id)

        self.assertEqual(personnel.first_name, PERSONNEL_REQUIRED.get("first_name"))
        self.assertEqual(personnel.last_name, PERSONNEL_REQUIRED.get("last_name"))
        self.assertEqual(personnel.pseudonym, PERSONNEL_REQUIRED.get("pseudonym"))

    def test_find_by_id(self):
        service = ServiceProvider.get(PersonnelService)
        id = service.new(**PERSONNEL_REQUIRED)
        personnel = service.find_by_id(id)

        self.assertEqual(personnel.first_name, PERSONNEL_REQUIRED.get("first_name"))
        self.assertEqual(personnel.last_name, PERSONNEL_REQUIRED.get("last_name"))
        self.assertEqual(personnel.pseudonym, PERSONNEL_REQUIRED.get("pseudonym"))

    def test_find_by_id_not_found(self):
        service = ServiceProvider.get(PersonnelService)
        id = 44_444

        with self.assertRaises(NotFoundError):
            _ = service.find_by_id(id)

    def test_find_by_ids(self):
        service = ServiceProvider.get(PersonnelService)
        ids: MutableSequence[Id] = []
        for _ in range(5):
            ids.append(service.new(**PERSONNEL_REQUIRED))

        found_personnel = service.find_by_ids(ids)

        for i, personnel in enumerate(found_personnel):
            self.assertEqual(personnel.id, ids[i])

    def test_find_by_ids_not_found(self):
        service = ServiceProvider.get(PersonnelService)
        ids = [44_444, 444_444, 4_444_444]
        found_personnel = service.find_by_ids(ids)

        self.assertSequenceEqual(found_personnel, [])

    def test_find_simple(self):
        service = ServiceProvider.get(PersonnelService)
        personnel_id1 = service.new(
            **{
                "first_name": "FoundPersonnel1",
                "last_name": "lIkElAsTnAmE",
                "pseudonym": "Pseudonym",
            }
        )
        personnel_id2 = service.new(
            **{
                "first_name": "FoundPersonnel2",
                "last_name": "LiKeLaStNaMe",
                "pseudonym": "Pseudonym",
            }
        )
        _ = service.new(
            **{
                "first_name": "UnfoundPersonnel3",
                "last_name": "LastName",
                "pseudonym": "pseudonym",
            }
        )

        found_personnel = service.find(
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
        service, series_service, consumable_service = (
            ServiceProvider.get(PersonnelService),
            ServiceProvider.get(SeriesService),
            ServiceProvider.get(ConsumableService),
        )
        personnel_id1 = service.new(
            **{**PERSONNEL_REQUIRED, "first_name": "FoundPersonnel1"}
        )
        personnel_id2 = service.new(
            **{**PERSONNEL_REQUIRED, "first_name": "UnfoundPersonnel2"}
        )

        series_id = series_service.new(**{"name": "FoundSeries1"})

        consumable_id1 = consumable_service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": series_id,
                "end_date": datetime.fromtimestamp(44_444),
            }
        )
        consumable_id2 = consumable_service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": -1,
                "end_date": datetime.fromtimestamp(4_444),
            }
        )

        _ = consumable_service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id1)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role")],
        )
        _ = consumable_service.change_personnel(
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

        found_personnel = service.find(**complex_where)

        self.assertEqual(len(found_personnel), 1)
        personnel = found_personnel[0]
        self.assertEqual(personnel.id, personnel_id1)
        self.assertEqual(personnel.first_name, "FoundPersonnel1")

    def test_find_not_found(self):
        service = ServiceProvider.get(PersonnelService)
        _ = service.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
            }
        }

        found_personnel = service.find(**where)

        self.assertEqual(len(found_personnel), 0)

    def test_update(self):
        service = ServiceProvider.get(PersonnelService)
        id = service.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {"personnel": {"id": [WhereQuery(id)]}}
        apply: PersonnelApplyMapping = {
            "first_name": ApplyQuery("UpdatedFirstName"),
            "pseudonym": ApplyQuery("UpdatedPseudonym"),
        }

        updated_personnel = service.update(where, apply)

        self.assertEqual(len(updated_personnel), 1)
        personnel = service.find_by_id(updated_personnel[0])
        self.assertEqual(personnel.id, id)
        self.assertEqual(personnel.first_name, "UpdatedFirstName")
        self.assertEqual(personnel.pseudonym, "UpdatedPseudonym")

    def test_update_not_found(self):
        service = ServiceProvider.get(PersonnelService)
        _ = service.new(**PERSONNEL_REQUIRED)

        where: WhereMapping = {
            "personnel": {
                "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
            }
        }
        apply: PersonnelApplyMapping = {"first_name": ApplyQuery("UpdatedFirstName")}

        updated_personnel = service.update(where, apply)

        self.assertEqual(len(updated_personnel), 0)

    def test_delete(self):
        service = ServiceProvider.get(PersonnelService)
        _ = service.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "DeletePersonnel1",
            }
        )
        _ = service.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "DeletePersonnel2",
            }
        )
        _ = service.new(
            **{
                **PERSONNEL_REQUIRED,
                "first_name": "SharedFirstName",
                "last_name": "KeptPersonnel3",
            }
        )

        deleted_personnel_count = service.delete(
            **{
                "personnel": {
                    "first_name": [WhereQuery("SharedFirstName", WhereOperator.EQ)],
                    "last_name": [WhereQuery("DeletePersonnel", WhereOperator.LIKE)],
                }
            }
        )

        self.assertEqual(deleted_personnel_count, 2)

    def test_delete_not_found(self):
        service = ServiceProvider.get(PersonnelService)
        _ = service.new(**PERSONNEL_REQUIRED)

        deleted_personnel_count = service.delete(
            **{
                "personnel": {
                    "first_name": [WhereQuery("UnfoundPersonnel", WhereOperator.LIKE)]
                }
            }
        )

        self.assertEqual(deleted_personnel_count, 0)


if __name__ == "__main__":
    _ = unittest.main()

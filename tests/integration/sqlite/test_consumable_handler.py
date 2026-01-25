import unittest
from collections.abc import MutableSequence
from datetime import datetime

from consumptionbackend.database import (
    ApplyOperator,
    ApplyQuery,
    ConsumableApplyMapping,
    ConsumableService,
    PersonnelService,
    SeriesService,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import Id, Status
from consumptionbackend.utils import NotFoundError, ServiceProvider
from tests.test_data import CONSUMABLE_REQUIRED, PERSONNEL_REQUIRED, SERIES_REQUIRED

from .base import SQLiteIntegrationTestBase


class TestConsumableIntegration(SQLiteIntegrationTestBase):
    def test_new_simple(self):
        service = ServiceProvider.get(ConsumableService)
        id = service.new(**{**CONSUMABLE_REQUIRED, "tags": ["tag1", "tag2"]})
        consumable = service.find_by_id(id)
        tags = service.tags(id)

        self.assertEqual(consumable.series_id, CONSUMABLE_REQUIRED.get("series_id"))
        self.assertEqual(consumable.name, CONSUMABLE_REQUIRED.get("name"))
        self.assertEqual(consumable.type, CONSUMABLE_REQUIRED.get("type"))
        self.assertEqual(consumable.status, CONSUMABLE_REQUIRED.get("status"))
        self.assertEqual(consumable.parts, CONSUMABLE_REQUIRED.get("parts"))
        self.assertEqual(consumable.max_parts, CONSUMABLE_REQUIRED.get("max_parts"))
        self.assertEqual(consumable.completions, CONSUMABLE_REQUIRED.get("completions"))
        self.assertEqual(consumable.rating, CONSUMABLE_REQUIRED.get("rating"))
        self.assertEqual(consumable.start_date, CONSUMABLE_REQUIRED.get("start_date"))
        self.assertEqual(consumable.end_date, CONSUMABLE_REQUIRED.get("end_date"))
        self.assertEqual(set(tags), {"tag1", "tag2"})

    def test_find_by_id(self):
        service = ServiceProvider.get(ConsumableService)
        id = service.new(**CONSUMABLE_REQUIRED)
        consumable = service.find_by_id(id)

        self.assertEqual(consumable.series_id, CONSUMABLE_REQUIRED.get("series_id"))
        self.assertEqual(consumable.name, CONSUMABLE_REQUIRED.get("name"))
        self.assertEqual(consumable.type, CONSUMABLE_REQUIRED.get("type"))
        self.assertEqual(consumable.status, CONSUMABLE_REQUIRED.get("status"))
        self.assertEqual(consumable.parts, CONSUMABLE_REQUIRED.get("parts"))
        self.assertEqual(consumable.max_parts, CONSUMABLE_REQUIRED.get("max_parts"))
        self.assertEqual(consumable.completions, CONSUMABLE_REQUIRED.get("completions"))
        self.assertEqual(consumable.rating, CONSUMABLE_REQUIRED.get("rating"))
        self.assertEqual(consumable.start_date, CONSUMABLE_REQUIRED.get("start_date"))
        self.assertEqual(consumable.end_date, CONSUMABLE_REQUIRED.get("end_date"))

    def test_find_by_id_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        id = 44_444

        with self.assertRaises(NotFoundError):
            _ = service.find_by_id(id)

    def test_find_by_ids(self):
        service = ServiceProvider.get(ConsumableService)
        ids: MutableSequence[Id] = []
        for _ in range(5):
            ids.append(service.new(**CONSUMABLE_REQUIRED))

        found_consumable = service.find_by_ids(ids)

        for i, consumable in enumerate(found_consumable):
            self.assertEqual(consumable.id, ids[i])

    def test_find_by_ids_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        ids = [44_444, 444_444, 4_444_444]
        found_series = service.find_by_ids(ids)

        self.assertSequenceEqual(found_series, [])

    def test_find_simple(self):
        service = ServiceProvider.get(ConsumableService)
        consumable_id1 = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "name": "FindSimpleConsumable1",
                "type": "FoundType",
                "status": Status.ON_HOLD,
                "completions": 44,
                "start_date": datetime.fromtimestamp(44_444_444),
            }
        )
        consumable_id2 = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "name": "FindSimpleConsumable2",
                "type": "FoundType",
                "status": Status.ON_HOLD,
                "completions": 4,
                "start_date": datetime.fromtimestamp(444_444_444),
            }
        )
        _ = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "name": "UnfoundSimpleConsumable3",
                "type": "UnfoundType",
                "status": Status.PLANNING,
                "completions": 444,
                "start_date": datetime.fromtimestamp(4_444_444),
            }
        )

        where: WhereMapping = {
            "consumables": {
                "name": [WhereQuery("simpleconsumable", WhereOperator.LIKE)],
                "type": [WhereQuery("FOUNDTYPE", WhereOperator.EQ)],
                "status": [WhereQuery(Status.ON_HOLD, WhereOperator.EQ)],
                "completions": [WhereQuery(44, WhereOperator.LTE)],
                "start_date": [
                    WhereQuery(datetime.fromtimestamp(44_444_443), WhereOperator.GT)
                ],
            }
        }

        found_consumable = service.find(**where)

        self.assertEqual(len(found_consumable), 2)
        self.assertSingle(
            found_consumable,
            lambda x: x.id == consumable_id1
            and x.name == "FindSimpleConsumable1"
            and x.type == "FOUNDTYPE"
            and x.status == Status.ON_HOLD
            and x.completions == 44
            and x.start_date == datetime.fromtimestamp(44_444_444),
        )
        self.assertSingle(
            found_consumable,
            lambda x: x.id == consumable_id2
            and x.name == "FindSimpleConsumable2"
            and x.type == "FOUNDTYPE"
            and x.status == Status.ON_HOLD
            and x.completions == 4
            and x.start_date == datetime.fromtimestamp(444_444_444),
        )

    def test_find_complex(self):
        service, series_service, personnel_service = (
            ServiceProvider.get(ConsumableService),
            ServiceProvider.get(SeriesService),
            ServiceProvider.get(PersonnelService),
        )
        series_id1 = series_service.new(**{"name": "FoundSeries1"})
        series_id2 = series_service.new(**{"name": "UnfoundSeries2"})

        consumable_id1 = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.COMPLETED,
                "series_id": series_id1,
                "tags": ["tag1", "tag2"],
            }
        )
        consumable_wrong_status = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.ON_HOLD,
                "series_id": series_id1,
                "tags": ["tag1", "tag2"],
            }
        )
        consumable_wrong_series = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.COMPLETED,
                "series_id": series_id2,
                "tags": ["tag1", "tag2"],
            }
        )
        consumable_wrong_tag1 = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.COMPLETED,
                "series_id": series_id1,
                "tags": ["tag1", "tag2", "tag3"],
            }
        )
        consumable_wrong_tag2 = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.COMPLETED,
                "series_id": series_id1,
                "tags": ["tag2"],
            }
        )
        _consumable_wrong_personnel = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "status": Status.COMPLETED,
                "series_id": series_id1,
                "tags": ["tag1", "tag2"],
            }
        )

        personnel_id = personnel_service.new(
            **{**PERSONNEL_REQUIRED, "last_name": "FoundPersonnel1"}
        )

        for consumable_id in [
            consumable_id1,
            consumable_wrong_status,
            consumable_wrong_series,
            consumable_wrong_tag1,
            consumable_wrong_tag2,
        ]:
            _ = service.change_personnel(
                {"consumables": {"id": [WhereQuery(consumable_id)]}},
                {"personnel": {"id": [WhereQuery(personnel_id)]}},
                [ApplyQuery("Role")],
            )

        complex_where: WhereMapping = {
            "consumables": {
                "status": [WhereQuery(Status.COMPLETED, WhereOperator.EQ)],
                "tags": [
                    WhereQuery("tag1", WhereOperator.EQ),
                    WhereQuery("tag2", WhereOperator.EQ),
                    WhereQuery("tag3", WhereOperator.NEQ),
                ],
            },
            "personnel": {
                "last_name": [WhereQuery("FoundPersonnel1", WhereOperator.EQ)],
                "role": [WhereQuery("Role", WhereOperator.LIKE)],
            },
            "series": {"name": [WhereQuery("foundseries1", WhereOperator.LIKE)]},
        }

        found_consumables = service.find(**complex_where)

        self.assertEqual(len(found_consumables), 1)
        consumable = found_consumables[0]
        self.assertEqual(consumable.id, consumable_id1)
        self.assertEqual(consumable.status, Status.COMPLETED)
        self.assertEqual(consumable.series_id, series_id1)

    def test_find_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        _ = service.new(**CONSUMABLE_REQUIRED)

        where: WhereMapping = {
            "consumables": {
                "name": [WhereQuery("UnfoundConsumable1", WhereOperator.LIKE)]
            }
        }

        found_consumable = service.find(**where)

        self.assertEqual(len(found_consumable), 0)

    def test_update(self):
        service = ServiceProvider.get(ConsumableService)
        id = service.new(
            **{
                **CONSUMABLE_REQUIRED,
                "parts": 44,
                "completions": 1,
                "rating": 0.44,
                "tags": ["tag1", "tag2"],
            }
        )

        where: WhereMapping = {"consumables": {"id": [WhereQuery(id)]}}
        apply: ConsumableApplyMapping = {
            "name": ApplyQuery("UpdatedConsumable", ApplyOperator.APPLY),
            "type": ApplyQuery("updatedtype", ApplyOperator.APPLY),
            "status": ApplyQuery(Status.PLANNING, ApplyOperator.APPLY),
            "parts": ApplyQuery(44_444, ApplyOperator.ADD),
            "max_parts": ApplyQuery(444_444, ApplyOperator.APPLY),
            "completions": ApplyQuery(4, ApplyOperator.ADD),
            "rating": ApplyQuery(44.44, ApplyOperator.ADD),
            "tags": [
                ApplyQuery("tag4", ApplyOperator.ADD),
                ApplyQuery("tag44", ApplyOperator.APPLY),
                ApplyQuery("tag1", ApplyOperator.SUB),
            ],
        }

        updated_consumable = service.update(where, apply)

        self.assertEqual(len(updated_consumable), 1)
        consumable = service.find_by_id(updated_consumable[0])
        self.assertEqual(consumable.id, id)
        self.assertEqual(consumable.name, "UpdatedConsumable")
        self.assertEqual(consumable.type, "UPDATEDTYPE")
        self.assertEqual(consumable.status, Status.PLANNING)
        self.assertEqual(consumable.parts, 44_488)
        self.assertEqual(consumable.max_parts, 444_444)
        self.assertEqual(consumable.completions, 5)
        if consumable.rating is None:
            raise Exception()
        self.assertAlmostEqual(consumable.rating, 44.88, 2)
        tags = service.tags(updated_consumable[0])
        self.assertEqual(set(tags), {"tag2", "tag4", "tag44"})

    def test_update_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        _ = service.new(**CONSUMABLE_REQUIRED)

        where: WhereMapping = {
            "consumables": {
                "name": [WhereQuery("UnfoundConsumable1", WhereOperator.LIKE)]
            }
        }
        apply: ConsumableApplyMapping = {"name": ApplyQuery("UpdatedConsumable")}

        updated_consumable = service.update(where, apply)

        self.assertEqual(len(updated_consumable), 0)

    def test_delete(self):
        service = ServiceProvider.get(ConsumableService)
        _ = service.new(**{**CONSUMABLE_REQUIRED, "name": "DeleteConsumable1"})
        _ = service.new(**{**CONSUMABLE_REQUIRED, "name": "DeleteConsumable2"})
        _ = service.new(**{**CONSUMABLE_REQUIRED, "name": "KeptConsumable3"})

        deleted_series_count = service.delete(
            **{
                "consumables": {
                    "name": [WhereQuery("deleteconsumable", WhereOperator.LIKE)]
                }
            }
        )

        self.assertEqual(deleted_series_count, 2)

    def test_delete_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        _ = service.new(**CONSUMABLE_REQUIRED)

        deleted_consumable_count = service.delete(
            **{
                "consumables": {
                    "name": [WhereQuery("UnfoundConsumable1", WhereOperator.LIKE)]
                }
            }
        )

        self.assertEqual(deleted_consumable_count, 0)

    def test_series(self):
        service, series_service = (
            ServiceProvider.get(ConsumableService),
            ServiceProvider.get(SeriesService),
        )
        series_id = series_service.new(**SERIES_REQUIRED)
        consumable_id = service.new(**{**CONSUMABLE_REQUIRED, "series_id": series_id})

        series = service.series(consumable_id)

        self.assertEqual(series.id, series_id)

    def test_personnel(self):
        service, personnel_service = (
            ServiceProvider.get(ConsumableService),
            ServiceProvider.get(PersonnelService),
        )
        consumable_id = service.new(**CONSUMABLE_REQUIRED)
        personnel_id1 = personnel_service.new(**PERSONNEL_REQUIRED)
        personnel_id2 = personnel_service.new(**PERSONNEL_REQUIRED)

        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role1"), ApplyQuery("Role2")],
        )
        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id2)]}},
            [ApplyQuery("Role3"), ApplyQuery("Role4")],
        )

        personnel = service.personnel(consumable_id)

        self.assertSingle(
            personnel,
            lambda x: x.id == personnel_id1 and set(x.roles) == {"Role1", "Role2"},
        )
        self.assertSingle(
            personnel,
            lambda x: x.id == personnel_id2 and set(x.roles) == {"Role3", "Role4"},
        )

    def test_personnel_not_found(self):
        service, personnel_service = (
            ServiceProvider.get(ConsumableService),
            ServiceProvider.get(PersonnelService),
        )
        consumable_id = service.new(**CONSUMABLE_REQUIRED)
        _ = personnel_service.new(**PERSONNEL_REQUIRED)
        _ = personnel_service.new(**PERSONNEL_REQUIRED)

        personnel = service.personnel(consumable_id)

        self.assertEqual(len(personnel), 0)

    def test_tags(self):
        service = ServiceProvider.get(ConsumableService)
        consumable_id = service.new(
            **{**CONSUMABLE_REQUIRED, "tags": ["tag1", "tag2", "tag3"]}
        )

        tags = service.tags(consumable_id)

        self.assertEqual(set(tags), {"tag1", "tag2", "tag3"})

    def test_tags_not_found(self):
        service = ServiceProvider.get(ConsumableService)
        consumable_id = service.new(**CONSUMABLE_REQUIRED)

        tags = service.tags(consumable_id)

        self.assertEqual(len(tags), 0)

    def test_change_personnel(self):
        service, personnel_service = (
            ServiceProvider.get(ConsumableService),
            ServiceProvider.get(PersonnelService),
        )
        consumable_id = service.new(**CONSUMABLE_REQUIRED)
        personnel_id1 = personnel_service.new(**PERSONNEL_REQUIRED)
        personnel_id2 = personnel_service.new(**PERSONNEL_REQUIRED)

        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role1"), ApplyQuery("Role2")],
        )
        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id2)]}},
            [ApplyQuery("Role3"), ApplyQuery("Role4")],
        )

        personnel1 = service.personnel(consumable_id)

        self.assertSingle(
            personnel1,
            lambda x: x.id == personnel_id1 and set(x.roles) == {"Role1", "Role2"},
        )
        self.assertSingle(
            personnel1,
            lambda x: x.id == personnel_id2 and set(x.roles) == {"Role3", "Role4"},
        )

        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role5", ApplyOperator.ADD)],
        )
        _ = service.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id)]}},
            {"personnel": {"id": [WhereQuery(personnel_id2)]}},
            [ApplyQuery("Role4", ApplyOperator.SUB)],
        )

        personnel2 = service.personnel(consumable_id)

        self.assertSingle(
            personnel2,
            lambda x: x.id == personnel_id1
            and set(x.roles) == {"Role1", "Role2", "Role5"},
        )
        self.assertSingle(
            personnel2,
            lambda x: x.id == personnel_id2 and set(x.roles) == {"Role3"},
        )


if __name__ == "__main__":
    _ = unittest.main()

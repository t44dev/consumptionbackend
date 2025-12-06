import unittest
from collections.abc import MutableSequence

from consumptionbackend.database import (
    ApplyQuery,
    SeriesApplyMapping,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.database.sqlite import (
    ConsumableHandler,
    PersonnelHandler,
    SeriesHandler,
)
from consumptionbackend.entities import Id, Status
from consumptionbackend.utils import NotFoundException
from tests.test_data import CONSUMABLE_REQUIRED, PERSONNEL_REQUIRED, SERIES_REQUIRED

from .base import SQLiteIntegrationTestBase


class TestSeriesIntegration(SQLiteIntegrationTestBase):
    def test_new_simple(self):
        id = SeriesHandler.new(**SERIES_REQUIRED)
        series = SeriesHandler.find_by_id(id)

        self.assertEqual(series.name, SERIES_REQUIRED.get("name"))

    def test_find_by_id(self):
        id = SeriesHandler.new(**SERIES_REQUIRED)
        series = SeriesHandler.find_by_id(id)

        self.assertEqual(series.name, SERIES_REQUIRED.get("name"))

    def test_find_by_id_not_found(self):
        id = 44_444

        with self.assertRaises(NotFoundException):
            _ = SeriesHandler.find_by_id(id)

    def test_find_by_ids(self):
        ids: MutableSequence[Id] = []
        for _ in range(5):
            ids.append(SeriesHandler.new(**SERIES_REQUIRED))

        found_series = SeriesHandler.find_by_ids(ids)

        for i, series in enumerate(found_series):
            self.assertEqual(series.id, ids[i])

    def test_find_by_ids_not_found(self):
        ids = [44_444, 444_444, 4_444_444]
        found_series = SeriesHandler.find_by_ids(ids)

        self.assertSequenceEqual(found_series, [])

    def test_find_simple(self):
        _ = SeriesHandler.new(**{"name": "FindSimpleSeries1"})
        _ = SeriesHandler.new(**{"name": "FindSimpleSeries2"})
        _ = SeriesHandler.new(**{"name": "FindOtherSeries1"})

        found_series = SeriesHandler.find(
            **{"series": {"name": [WhereQuery("simpleseries", WhereOperator.LIKE)]}}
        )

        self.assertEqual(len(found_series), 2)
        self.assertSingle(found_series, lambda x: x.name == "FindSimpleSeries1")
        self.assertSingle(found_series, lambda x: x.name == "FindSimpleSeries2")

    def test_find_complex(self):
        series_id1 = SeriesHandler.new(**{"name": "FoundSeries1"})
        series_id2 = SeriesHandler.new(**{"name": "UnfoundSeries2"})
        series_id3 = SeriesHandler.new(**{"name": "UnfoundSeries3"})

        consumable_id1 = ConsumableHandler.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": series_id1,
                "status": Status.COMPLETED,
            }
        )
        consumable_id2 = ConsumableHandler.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": series_id2,
                "status": Status.COMPLETED,
            }
        )
        consumable_id3 = ConsumableHandler.new(
            **{
                **CONSUMABLE_REQUIRED,
                "series_id": series_id3,
                "status": Status.IN_PROGRESS,
            }
        )

        personnel_id1 = PersonnelHandler.new(
            **{**PERSONNEL_REQUIRED, "pseudonym": "FoundPersonnel1"}
        )
        personnel_id2 = PersonnelHandler.new(
            **{**PERSONNEL_REQUIRED, "pseudonym": "UnfoundPersonnel2"}
        )

        _ = ConsumableHandler.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id1)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role1")],
        )
        _ = ConsumableHandler.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id3)]}},
            {"personnel": {"id": [WhereQuery(personnel_id1)]}},
            [ApplyQuery("Role2")],
        )
        _ = ConsumableHandler.change_personnel(
            {"consumables": {"id": [WhereQuery(consumable_id2)]}},
            {"personnel": {"id": [WhereQuery(personnel_id2)]}},
            [ApplyQuery("Role1")],
        )

        complex_where: WhereMapping = {
            "consumables": {
                "status": [WhereQuery(Status.IN_PROGRESS, WhereOperator.GT)]
            },
            "personnel": {
                "pseudonym": [WhereQuery("FoundPersonnel1", WhereOperator.EQ)],
                "role": [WhereQuery("Role", WhereOperator.LIKE)],
            },
            "series": {"name": [WhereQuery("foundseries", WhereOperator.LIKE)]},
        }

        found_series = SeriesHandler.find(**complex_where)

        self.assertEqual(len(found_series), 1)
        series = found_series[0]
        self.assertEqual(series.id, series_id1)
        self.assertEqual(series.name, "FoundSeries1")

    def test_find_not_found(self):
        _ = SeriesHandler.new(**SERIES_REQUIRED)

        where: WhereMapping = {
            "series": {"name": [WhereQuery("UnfoundSeries1", WhereOperator.LIKE)]}
        }

        found_series = SeriesHandler.find(**where)

        self.assertEqual(len(found_series), 0)

    def test_update(self):
        id = SeriesHandler.new(**SERIES_REQUIRED)

        where: WhereMapping = {"series": {"id": [WhereQuery(id)]}}
        apply: SeriesApplyMapping = {"name": ApplyQuery("UpdatedSeries")}

        updated_series = SeriesHandler.update(where, apply)

        self.assertEqual(len(updated_series), 1)
        series = SeriesHandler.find_by_id(updated_series[0])
        self.assertEqual(series.id, id)
        self.assertEqual(series.name, "UpdatedSeries")

    def test_update_not_found(self):
        _ = SeriesHandler.new(**SERIES_REQUIRED)

        where: WhereMapping = {
            "series": {"name": [WhereQuery("UnfoundSeries1", WhereOperator.LIKE)]}
        }
        apply: SeriesApplyMapping = {"name": ApplyQuery("UpdatedSeries")}

        updated_series = SeriesHandler.update(where, apply)

        self.assertEqual(len(updated_series), 0)

    def test_delete(self):
        series_id = SeriesHandler.new(**{"name": "DeleteSeries1"})
        _ = SeriesHandler.new(**{"name": "DeleteSeries2"})
        _ = SeriesHandler.new(**{"name": "KeptSeries3"})

        consumable_id = ConsumableHandler.new(
            **{**CONSUMABLE_REQUIRED, "series_id": series_id}
        )

        deleted_series_count = SeriesHandler.delete(
            **{"series": {"name": [WhereQuery("deleteseries", WhereOperator.LIKE)]}}
        )

        self.assertEqual(deleted_series_count, 2)
        consumable = ConsumableHandler.find_by_id(consumable_id)
        self.assertEqual(consumable.series_id, -1)

    def test_delete_not_found(self):
        _ = SeriesHandler.new(**SERIES_REQUIRED)

        deleted_series_count = SeriesHandler.delete(
            **{"series": {"name": [WhereQuery("UnfoundSeries1", WhereOperator.LIKE)]}}
        )

        self.assertEqual(deleted_series_count, 0)

    def test_consumables(self):
        series_id1 = SeriesHandler.new(**SERIES_REQUIRED)
        series_id2 = SeriesHandler.new(**SERIES_REQUIRED)

        consumable_id1 = ConsumableHandler.new(
            **{**CONSUMABLE_REQUIRED, "series_id": series_id1}
        )
        consumable_id2 = ConsumableHandler.new(
            **{**CONSUMABLE_REQUIRED, "series_id": series_id1}
        )
        _ = ConsumableHandler.new(**{**CONSUMABLE_REQUIRED, "series_id": series_id2})
        _ = ConsumableHandler.new(**{**CONSUMABLE_REQUIRED, "series_id": -1})

        consumables = SeriesHandler.consumables(series_id1)

        self.assertEqual(len(consumables), 2)
        self.assertSingle(consumables, lambda x: x.id == consumable_id1)
        self.assertSingle(consumables, lambda x: x.id == consumable_id2)

    def test_consumables_not_found(self):
        series_id1 = SeriesHandler.new(**SERIES_REQUIRED)
        series_id2 = SeriesHandler.new(**SERIES_REQUIRED)

        _ = ConsumableHandler.new(**{**CONSUMABLE_REQUIRED, "series_id": series_id2})
        _ = ConsumableHandler.new(**{**CONSUMABLE_REQUIRED, "series_id": -1})

        consumables = SeriesHandler.consumables(series_id1)

        self.assertEqual(len(consumables), 0)


if __name__ == "__main__":
    _ = unittest.main()

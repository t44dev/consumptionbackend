import unittest

from consumptionbackend.database import ConsumableService, TagService
from consumptionbackend.utils import ServiceProvider
from tests.test_data import CONSUMABLE_REQUIRED

from .base import SQLiteIntegrationTestBase


class TestTagIntegration(SQLiteIntegrationTestBase):
    def test_find(self):
        service = ServiceProvider.get(TagService)
        consumable_service = ServiceProvider.get(ConsumableService)
        _ = consumable_service.new(**{**CONSUMABLE_REQUIRED, "tags": ["tag1", "tag2"]})
        _ = consumable_service.new(**{**CONSUMABLE_REQUIRED, "tags": ["tag2", "tag3"]})
        _ = consumable_service.new(**{**CONSUMABLE_REQUIRED, "tags": ["tag4", "tag5"]})
        found_tags = service.find()

        self.assertSingle(found_tags, lambda x: x == "tag1")
        self.assertSingle(found_tags, lambda x: x == "tag2")
        self.assertSingle(found_tags, lambda x: x == "tag3")
        self.assertSingle(found_tags, lambda x: x == "tag4")
        self.assertSingle(found_tags, lambda x: x == "tag5")

    def test_find_not_found(self):
        service = ServiceProvider.get(TagService)

        found_tags = service.find()

        self.assertEqual(len(found_tags), 0)


if __name__ == "__main__":
    _ = unittest.main()

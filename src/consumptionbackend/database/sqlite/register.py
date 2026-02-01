from pathlib import Path

from consumptionbackend.database import (
    ConsumableService,
    PersonnelService,
    SeriesService,
    TagService,
)
from consumptionbackend.database.sqlite.tag import SQLiteTagService
from consumptionbackend.utils import ServiceProvider

from .consumable import SQLiteConsumableService
from .engine import SQLiteDatabaseEngine, SQLiteFileDatabaseEngine
from .personnel import SQLitePersonnelService
from .series import SQLiteSeriesService


def register_sqlite_services(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ServiceProvider.register(SQLiteDatabaseEngine, SQLiteFileDatabaseEngine(db_path))
    ServiceProvider.register(ConsumableService, SQLiteConsumableService())
    ServiceProvider.register(SeriesService, SQLiteSeriesService())
    ServiceProvider.register(PersonnelService, SQLitePersonnelService())
    ServiceProvider.register(TagService, SQLiteTagService())

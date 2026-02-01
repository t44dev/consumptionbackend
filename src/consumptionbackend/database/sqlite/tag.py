import logging
import sqlite3
from collections.abc import Sequence
from typing import final, override

from consumptionbackend.database import TagService
from consumptionbackend.database.sqlite.helper import SQLiteDatabaseHelper
from consumptionbackend.utils import ServiceProvider

from .engine import SQLiteDatabaseEngine
from .sql_utils import SQLiteType, to_shorthand

logger = logging.getLogger(__name__)


@final
class SQLiteTagService(TagService):
    @override
    def find(self) -> Sequence[str]:
        db = ServiceProvider.get(SQLiteDatabaseEngine).db
        cur = db.cursor()

        results: Sequence[sqlite3.Row] = cur.execute(*(self._find_sql())).fetchall()

        cur.close()

        tags = [result["tag"] for result in results]
        logger.info("Found tags", extra={"data": {"results": tags}})

        return tags

    def _find_sql(self) -> tuple[str, Sequence[SQLiteType]]:
        sql = f"""
        SELECT DISTINCT tag 
            FROM {SQLiteDatabaseHelper.TAGS_MAPPING_TABLE} {to_shorthand(SQLiteDatabaseHelper.TAGS_MAPPING_TABLE)}
        """

        return sql, []

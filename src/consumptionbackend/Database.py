# General Imports
from __future__ import annotations  # For self-referential type-hints
from pathlib import Path
import os
from abc import abstractmethod, ABC
from typing import Union, Any
import json
import sqlite3
from collections.abc import Sequence, Mapping

# Consumption Imports
from .config_handling import CONFIG_PATH
from .Status import Status


class DatabaseHandler:
    DB_CONNECTION: sqlite3.Connection = None

    def __init__(self) -> None:
        raise RuntimeError("Class cannot be used outside of a static context.")

    @classmethod
    def get_db(cls) -> sqlite3.Connection:
        if not DatabaseHandler.DB_CONNECTION:
            with open(CONFIG_PATH, "r") as f:
                cfg = json.load(f)
                DB_PATH = Path(os.path.expanduser(cfg["DB_PATH"]))
                cls.DB_CONNECTION = sqlite3.connect(DB_PATH)
        return cls.DB_CONNECTION


class DatabaseEntity(ABC):
    handler: DatabaseHandler = DatabaseHandler

    def __init__(self, *args, id: Union[int, None] = None) -> None:
        super().__init__()
        # None if not presently in the database, else the internal db id.
        self.id = id

    @classmethod
    @abstractmethod
    def new(cls, **kwargs) -> DatabaseEntity:
        pass

    @classmethod
    @abstractmethod
    def find(cls, **kwargs) -> Sequence[DatabaseEntity]:
        pass

    @classmethod
    @abstractmethod
    def update(
        cls, where_map: Mapping[str, Any], set_map: Mapping[str, Any]
    ) -> Sequence[DatabaseEntity]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **kwargs) -> bool:
        pass

    def __eq__(self, other: DatabaseEntity) -> bool:
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class DatabaseInstantiator:
    def __init__(self) -> None:
        raise RuntimeError("Class cannot be used outside of a static context.")

    @classmethod
    def run(cls):
        cls.series_table()
        cls.personnel_table()
        cls.consumable_table()

    @classmethod
    def consumable_table(cls):
        sql = """CREATE TABLE IF NOT EXISTS consumables(
            id INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 0,
            series_id INTEGER NOT NULL DEFAULT -1,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            status INTEGER NOT NULL DEFAULT 0,
            parts INTEGER NOT NULL DEFAULT 0,
            max_parts INTEGER DEFAULT NULL,
            completions INTEGER NOT NULL DEFAULT 0,
            rating REAL,
            start_date REAL,
            end_date REAL,
            FOREIGN KEY (series_id)
                REFERENCES series (id)
                ON DELETE SET DEFAULT 
                ON UPDATE NO ACTION
            )"""
        sql_personnel_mapping = """CREATE TABLE IF NOT EXISTS consumable_personnel(
                            personnel_id INTEGER NOT NULL,
                            consumable_id INTEGER NOT NULL,
                            role TEXT,
                            PRIMARY KEY (personnel_id, consumable_id, role)
                            FOREIGN KEY (personnel_id)
                                REFERENCES personnel (id)
                                ON DELETE CASCADE
                                ON UPDATE NO ACTION
                            FOREIGN KEY (consumable_id)
                                REFERENCES consumables (id)
                                ON DELETE CASCADE
                                ON UPDATE NO ACTION
                            )"""
        sql_tag_mapping = """CREATE TABLE IF NOT EXISTS consumable_tags(
                        consumable_id INTEGER NOT NULL,
                        tag TEXT NOT NULL,
                        PRIMARY KEY (consumable_id, tag)
                        FOREIGN KEY (consumable_id)
                            REFERENCES consumables (id)
                            ON DELETE CASCADE
                            ON UPDATE NO ACTION
                        )"""
        DatabaseHandler.get_db().cursor().execute(sql)
        DatabaseHandler.get_db().cursor().execute(sql_personnel_mapping)
        DatabaseHandler.get_db().cursor().execute(sql_tag_mapping)
        cls._consumable_triggers()

    @classmethod
    def _consumable_triggers(cls):
        cur = DatabaseHandler.get_db().cursor()
        # Set completions if COMPLETED
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS completions_on_completed_update
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.completions = 0 AND NEW.status = {Status.COMPLETED.value}
                BEGIN
                    UPDATE consumables SET completions = 1 WHERE id = NEW.id;
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS completions_on_completed_insert
                AFTER INSERT ON consumables 
                WHEN NEW.completions = 0 AND NEW.status = {Status.COMPLETED.value}
                BEGIN
                    UPDATE consumables SET completions = 1 WHERE id = NEW.id; 
                END
        """
        )
        # Set start_date if IN_PROGRESS
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS start_date_on_in_progress_update 
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.start_date IS NULL AND NEW.status = {Status.IN_PROGRESS.value}
                BEGIN
                    UPDATE consumables SET start_date = strftime('%s') WHERE id = NEW.id; 
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS start_date_on_in_progress_insert 
                AFTER INSERT ON consumables 
                WHEN NEW.start_date IS NULL AND NEW.status = {Status.IN_PROGRESS.value}
                BEGIN
                    UPDATE consumables SET start_date = strftime('%s') WHERE id = NEW.id; 
                END
        """
        )
        # Set end_date if COMPLETED
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS end_date_on_completed_update 
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.end_date IS NULL AND NEW.status = {Status.COMPLETED.value} 
                BEGIN
                    UPDATE consumables SET end_date = strftime('%s') WHERE id = NEW.id; 
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS end_date_on_completed_insert
                AFTER INSERT ON consumables 
                WHEN NEW.end_date IS NULL AND NEW.status = {Status.COMPLETED.value} 
                BEGIN
                    UPDATE consumables SET end_date = strftime('%s') WHERE id = NEW.id; 
                END
        """
        )
        # Parts at least 1 on COMPLETED
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS parts_on_completed_update 
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.parts = 0 AND NEW.status = {Status.COMPLETED.value} AND NEW.max_parts IS NULL
                BEGIN
                    UPDATE consumables SET parts = 1 WHERE id = NEW.id; 
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS parts_on_completed_insert
                AFTER INSERT ON consumables 
                WHEN NEW.parts = 0 AND NEW.status = {Status.COMPLETED.value} AND NEW.max_parts IS NULL
                BEGIN
                    UPDATE consumables SET parts = 1 WHERE id = NEW.id; 
                END
        """
        )
        # Parts is max_parts on COMPLETED if max_parts exists
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS parts_on_completed_update_max
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.parts = 0 AND NEW.status = {Status.COMPLETED.value} AND NEW.max_parts IS NOT NULL
                BEGIN
                    UPDATE consumables SET parts = NEW.max_parts WHERE id = NEW.id; 
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS parts_on_completed_insert_max
                AFTER INSERT ON consumables 
                WHEN NEW.parts = 0 AND NEW.status = {Status.COMPLETED.value} AND NEW.max_parts IS NOT NULL
                BEGIN
                    UPDATE consumables SET parts = NEW.max_parts WHERE id = NEW.id; 
                END
        """
        )
        # Set Max Parts
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS max_parts_on_completed_update 
                AFTER UPDATE ON consumables 
                FOR EACH ROW
                WHEN NEW.max_parts IS NULL AND NEW.parts <> 0 AND NEW.status = {Status.COMPLETED.value}
                BEGIN
                    UPDATE consumables SET max_parts = NEW.parts WHERE id = NEW.id; 
                END
        """
        )
        cur.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS max_parts_on_completed_insert
                AFTER INSERT ON consumables 
                WHEN NEW.max_parts IS NULL AND NEW.parts <> 0 AND NEW.status = {Status.COMPLETED.value}
                BEGIN
                    UPDATE consumables SET max_parts = NEW.parts WHERE id = NEW.id; 
                END
        """
        )
        # Errors
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS date_error_update 
                AFTER UPDATE ON consumables 
                WHEN NEW.start_date IS NOT NULL AND NEW.end_date IS NOT NULL AND NEW.start_date > NEW.end_date
                BEGIN
                    SELECT RAISE(ROLLBACK, 'end date must be after start date');
                END
        """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS date_error_insert 
                AFTER INSERT ON consumables 
                WHEN NEW.start_date IS NOT NULL AND NEW.end_date IS NOT NULL AND NEW.start_date > NEW.end_date
                BEGIN
                    SELECT RAISE(ROLLBACK, 'end date must be after start date');
                END
        """
        )

    @classmethod
    def personnel_table(cls):
        sql = """CREATE TABLE IF NOT EXISTS personnel(
            id INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 0,
            first_name TEXT,
            last_name TEXT,
            pseudonym TEXT
        )"""
        DatabaseHandler.get_db().cursor().execute(sql)

    @classmethod
    def series_table(cls):
        cur = DatabaseHandler.get_db().cursor()
        sql = """CREATE TABLE IF NOT EXISTS series(
            id INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 0,
            name TEXT
        )"""
        cur.execute(sql)
        # None Series must be in database
        cur.execute("SELECT * FROM series WHERE id = -1")
        if len(cur.fetchall()) == 0:
            cur.execute("INSERT INTO series (id, name) VALUES (-1, 'None')")
            DatabaseHandler.get_db().commit()
        cls._series_triggers()

    @classmethod
    def _series_triggers(cls):
        cur = DatabaseHandler.get_db().cursor()
        # Cannot delete ID = -1
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS delete_none_series 
                BEFORE DELETE ON series 
                FOR EACH ROW
                WHEN OLD.id = -1 
                BEGIN
                    SELECT RAISE(ROLLBACK, 'cannot delete series with ID -1');
                END
        """
        )

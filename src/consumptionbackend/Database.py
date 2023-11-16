# General Imports
from __future__ import annotations  # For self-referential type-hints
from pathlib import Path
import os
from abc import abstractmethod, ABC
from typing import Union, Any
import json
import sqlite3
from collections.abc import Sequence, Mapping

# Package Imports
from .path_handling import CONFIG_PATH


class DatabaseHandler():

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

    db: sqlite3.Connection = DatabaseHandler.get_db()

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
    def update(cls, where: Mapping[str, Any], set: Mapping[str, Any]) -> Sequence[DatabaseEntity]:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, **kwargs) -> bool:
        pass

    def __eq__(self, other: DatabaseEntity) -> bool:
        return self.id == other.id


class DatabaseInstantiator():

    def __init__(self) -> None:
        raise RuntimeError("Class cannot be used outside of a static context.")

    @classmethod
    def run(cls):
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
            completions INTEGER NOT NULL DEFAULT 0,
            rating REAL,
            start_date REAL,
            end_date REAL,
            FOREIGN KEY (series_id)
                REFERENCES series (id)
                ON DELETE CASCADE
                ON UPDATE NO ACTION
            )"""
        sql_personnel_mapping = """CREATE TABLE IF NOT EXISTS consumable_personnel(
                            personnel_id INTEGER NOT NULL,
                            consumable_id INTEGER NOT NULL,
                            role TEXT,
                            PRIMARY KEY (personnel_id, consumable_id)
                            FOREIGN KEY (personnel_id)
                                REFERENCES personnel (id)
                                ON DELETE CASCADE
                                ON UPDATE NO ACTION
                            FOREIGN KEY (consumable_id)
                                REFERENCES consumables (id)
                                ON DELETE CASCADE
                                ON UPDATE NO ACTION
                            )"""
        DatabaseHandler.get_db().cursor().execute(sql)
        DatabaseHandler.get_db().cursor().execute(sql_personnel_mapping)

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
        sql = """CREATE TABLE IF NOT EXISTS series(
            id INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 0,
            name TEXT
        )"""
        DatabaseHandler.get_db().cursor().execute(sql)

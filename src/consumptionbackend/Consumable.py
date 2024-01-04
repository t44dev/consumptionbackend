# General Imports
from __future__ import annotations
import logging
from typing import Union, Any
from datetime import datetime
from collections.abc import Sequence, Mapping

# Consumption Imports
from . import Database
from . import Personnel as pers
from . import Series as ser
from .Status import Status


class Consumable(Database.DatabaseEntity):
    DB_NAME = "consumables"
    DB_PERSONNEL_MAPPING_NAME = "consumable_personnel"
    DB_TAG_MAPPING_NAME = "consumable_tags"

    def __init__(
        self,
        *args,
        id: Union[int, None] = None,
        series_id: int = -1,
        name: str = "",
        type: str = "",
        status: Union[Status, int] = Status.PLANNING,
        parts: int = 0,
        max_parts: Union[int, None] = None,
        completions: int = 0,
        rating: Union[float, None] = None,
        start_date: Union[float, None] = None,
        end_date: Union[float, None] = None,
    ) -> None:
        super().__init__(*args, id=id)
        self.series_id = series_id
        self.name = name
        self.type = type.upper()
        self.status = Status(status) if not isinstance(status, Status) else status
        self.parts = parts
        self.max_parts = max_parts
        self.completions = completions
        self.rating = rating
        self.start_date = start_date
        self.end_date = end_date
        self._enforce_constraints()

    def _enforce_constraints(self) -> None:
        # Convert status to Enum
        if not isinstance(self.status, Status):
            self.status = Status(self.status)
        # Set completions if COMPLETED
        if self.completions == 0 and self.status == Status.COMPLETED:
            self.completions = 1
        # Set start_date if IN_PROGRESS
        if self.start_date is None and self.status == Status.IN_PROGRESS:
            self.start_date = datetime.utcnow().timestamp()  # Posix-timestamp
        # Set end_date if COMPLETED
        if self.end_date is None and self.status == Status.COMPLETED:
            self.end_date = datetime.utcnow().timestamp()  # Posix-timestamp
        # Parts at least 1 if COMPLETED
        if self.parts == 0 and self.status == Status.COMPLETED:
            self.parts = 1 if self.max_parts is None else self.max_parts
        # Max Parts = Parts if None and COMPLETED
        if self.status == Status.COMPLETED and self.max_parts is None:
            self.max_parts = self.parts

    def get_series(self) -> ser.Series:
        return ser.Series.find(id=self.series_id)[0]

    def set_series(self, series: ser.Series, do_log: bool = True) -> bool:
        self.update({"id": self.id}, {"series_id": series.id}, do_log=False)
        # Logging
        if do_log:
            logging.getLogger(__name__).info(f"SET_SERIES#{self.id},{series.id}")

    def get_tags(self) -> Sequence[str]:
        cur = self.handler.get_db().cursor()
        sql = f"""SELECT tag FROM {Consumable.DB_TAG_MAPPING_NAME} 
                WHERE consumable_id = ?"""
        cur.execute(sql, [self.id])
        return list(map(lambda x: x[0], cur.fetchall()))

    def add_tag(self, tag: str, do_log: bool = True) -> bool:
        tag = tag.strip().lower()
        if tag in self.get_tags():
            return False
        cur = self.handler.get_db().cursor()
        sql = f"INSERT INTO {Consumable.DB_TAG_MAPPING_NAME} (consumable_id, tag) values (?,?)"
        cur.execute(sql, [self.id, tag])
        self.handler.get_db().commit()
        # Logging
        if do_log:
            logging.getLogger(__name__).info(f"ADD_TAG#{self.id},'{tag}'")
        return True

    def remove_tag(self, tag: str, do_log: bool = True) -> bool:
        tag = tag.strip().lower()
        cur = self.handler.get_db().cursor()
        sql = f"""DELETE FROM {Consumable.DB_TAG_MAPPING_NAME} 
                WHERE consumable_id = ? AND tag = ?"""
        cur.execute(sql, [self.id, tag])
        self.handler.get_db().commit()
        # Logging
        if do_log:
            logging.getLogger(__name__).info(f"REMOVE_TAG#{self.id},'{tag}'")
        return True

    def get_personnel(self) -> Sequence[pers.Personnel]:
        if self.id is None:
            raise ValueError("Cannot find Personnel for Consumable without ID.")
        cur = self.handler.get_db().cursor()
        sql = f"""SELECT * FROM {Consumable.DB_PERSONNEL_MAPPING_NAME} 
                    LEFT JOIN {pers.Personnel.DB_NAME} 
                    ON {Consumable.DB_PERSONNEL_MAPPING_NAME}.personnel_id = {pers.Personnel.DB_NAME}.id
                    WHERE consumable_id = ?
                """
        cur.execute(sql, [self.id])
        rows = cur.fetchall()
        personnel = []
        for row in rows:
            personnel.append(
                pers.Personnel(
                    id=row[3],
                    first_name=row[4],
                    last_name=row[5],
                    pseudonym=row[6],
                    role=row[2],
                )
            )
        return personnel

    def add_personnel(self, personnel: pers.Personnel, do_log: bool = True) -> bool:
        if self.id is None:
            raise ValueError("Cannot assign Personnel to Consumable without ID.")
        if personnel.id is None:
            raise ValueError("Cannot assign a Personnel to Consumable without an ID.")
        if not personnel.role:
            raise ValueError(
                "Cannot assign Personnel to Consumable without assigned role."
            )
        cur = self.handler.get_db().cursor()
        sql = f"INSERT INTO {self.DB_PERSONNEL_MAPPING_NAME} (personnel_id, consumable_id, role) VALUES (?,?,?)"
        cur.execute(sql, [personnel.id, self.id, personnel.role])
        self.handler.get_db().commit()
        # Logging
        if do_log:
            logging.getLogger(__name__).info(
                f"ADD_PERSONNEL#{self.id},{personnel.id},'{personnel.role}'"
            )
        return True

    def remove_personnel(self, personnel: pers.Personnel, do_log: bool = True) -> bool:
        if self.id is None:
            raise ValueError("Cannot remove Personnel from Consumable without ID.")
        if personnel.id is None:
            raise ValueError("Cannot remove a Personnel from Consumable without an ID.")
        if not personnel.role:
            raise ValueError(
                "Cannot remove Personnel from Consumable without assigned role."
            )
        cur = self.handler.get_db().cursor()
        sql = f"""DELETE FROM {self.DB_PERSONNEL_MAPPING_NAME} 
                WHERE personnel_id = ? AND consumable_id = ? AND role = ?"""
        cur.execute(sql, [personnel.id, self.id, personnel.role])
        self.handler.get_db().commit()
        # Logging
        if do_log:
            logging.getLogger(__name__).info(
                f"REMOVE_PERSONNEL#{self.id},{personnel.id},'{personnel.role}'"
            )
        return True

    @classmethod
    def _assert_attrs(cls, d: Mapping[str, Any], tags: bool = True) -> None:
        attrs = {
            "id",
            "series_id",
            "name",
            "type",
            "status",
            "parts",
            "max_parts",
            "completions",
            "rating",
            "start_date",
            "end_date",
        }
        if tags:
            attrs.add("tags")
        for key in d.keys():
            if key not in attrs:
                raise ValueError(
                    f"Improper key provided in attribute mapping for Consumable: {key}"
                )

    @classmethod
    def _seq_to_consumable(cls, seq: Sequence[Any]) -> Consumable:
        return Consumable(
            id=seq[0],
            series_id=seq[1],
            name=seq[2],
            type=seq[3],
            status=seq[4],
            parts=seq[5],
            max_parts=seq[6],
            completions=seq[7],
            rating=seq[8],
            start_date=seq[9],
            end_date=seq[10],
        )

    @classmethod
    def _consumable_to_seq(cls, cons: Consumable) -> Sequence[Any]:
        return [
            cons.id,
            cons.series_id,
            cons.name,
            cons.type,
            cons.status.value,
            cons.parts,
            cons.max_parts,
            cons.completions,
            cons.rating,
            cons.start_date,
            cons.end_date,
        ]

    @classmethod
    def _filter_by_tags(cls, tags: Sequence[str]) -> str:
        templating = ",".join(["?" for _ in tags])
        sql = f"""SELECT * FROM {Consumable.DB_NAME} 
            WHERE id IN 
                (SELECT consumable_id 
                    FROM {Consumable.DB_TAG_MAPPING_NAME} 
                    WHERE tag IN ({templating})
                    GROUP BY consumable_id
                    HAVING COUNT(*) = {len(tags)}
                )
            """
        return sql

    @classmethod
    def new(cls, do_log: bool = True, **kwargs) -> Consumable:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        consumable = Consumable(**kwargs)
        sql = f"""INSERT INTO {cls.DB_NAME} 
                (id, series_id, name, type, status, parts, max_parts, completions, rating, start_date, end_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """
        cur.execute(sql, cls._consumable_to_seq(consumable))
        cls.handler.get_db().commit()
        consumable.id = cur.lastrowid
        # Logging
        if do_log:
            logging.getLogger(__name__).info(f"NEW_CONSUMABLE#{consumable._csv_str()}")
        return consumable

    @classmethod
    def find(cls, **kwargs) -> Sequence[Consumable]:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        if "tags" in kwargs:
            values = kwargs["tags"]
            db_name = cls._filter_by_tags(values)
            del kwargs["tags"]
        else:
            values = []
            db_name = Consumable.DB_NAME

        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            elif key == "type":
                where.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where.append(f"{key} = ?")
                values.append(value.value)
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"SELECT * FROM ({db_name}) WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        rows = cur.fetchall()
        consumables = []
        for row in rows:
            consumables.append(cls._seq_to_consumable(row))
        return consumables

    @classmethod
    def update(
        cls,
        where_map: Mapping[str, Any],
        set_map: Mapping[str, Any],
        do_log: bool = True,
    ) -> Sequence[Consumable]:
        if len(set_map) == 0:
            raise ValueError("Set map cannot be empty.")
        cls._assert_attrs(where_map)
        cls._assert_attrs(set_map)
        old_consumables = {c.id: c for c in cls.find(**where_map.copy())}
        cur = cls.handler.get_db().cursor()
        if "tags" in where_map:
            values = where_map["tags"]
            db_name = cls._filter_by_tags(values)
            del where_map["tags"]
        else:
            values = []
            db_name = Consumable.DB_NAME

        set_placeholders = []
        for key, value in set_map.items():
            if key == "type":
                set_placeholders.append(f"{key} = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                set_placeholders.append(f"{key} = ?")
                values.append(value.value)
            else:
                set_placeholders.append(f"{key} = ?")
                values.append(value)

        where_placeholders = ["true"]
        for key, value in where_map.items():
            if key == "name":
                where_placeholders.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            elif key == "type":
                where_placeholders.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where_placeholders.append(f"{key} = ?")
                values.append(value.value)
            else:
                where_placeholders.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE {db_name} SET {', '.join(set_placeholders)} WHERE {' AND '.join(where_placeholders)} RETURNING *"
        cur.execute(sql, values)
        rows = cur.fetchall()
        cls.handler.get_db().commit()
        consumables = []
        for row in rows:
            new_consumable = cls._seq_to_consumable(row)
            consumables.append(new_consumable)
            # Logging
            if do_log:
                old_consumable = old_consumables.get(new_consumable.id)
                logging.getLogger(__name__).info(
                    f"UPDATE_CONSUMABLE#{old_consumable._csv_str()}#{new_consumable._csv_str()}"
                )
        return consumables

    @classmethod
    def delete(cls, do_log: bool = True, **kwargs) -> bool:
        cls._assert_attrs(kwargs)
        old_consumables = cls.find(**kwargs.copy())
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        if "tags" in where:
            values = where["tags"]
            db_name = cls._filter_by_tags(values)
            del where["tags"]
        else:
            values = []
            db_name = Consumable.DB_NAME

        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            elif key == "type":
                where.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where.append(f"{key} = ?")
                values.append(value.value)
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"DELETE FROM {db_name} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        cls.handler.get_db().commit()
        if do_log:
            logger = logging.getLogger(__name__)
            for consumable in old_consumables:
                logger.info(f"DELETE_CONSUMABLE#{consumable._csv_str()}")
        return True

    def update_self(self, set_map: Mapping[str, Any]) -> Consumable:
        if self.id is None:
            raise ValueError("Cannot update Consumable that does not have an ID.")
        update = self.update({"id": self.id}, set_map)
        assert len(update) == 1
        return update[0]

    def delete_self(self) -> bool:
        if self.id is None:
            raise ValueError("Cannot delete Consumable that does not have an ID.")
        return self.delete(id=self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} | {self.name} with ID: {self.id}"

    def __str__(self) -> str:
        return f"[{self.type}] {self.name}"

    def _csv_str(self) -> str:
        return f"{self.id},{self.series_id},'{self.name}','{self.type}',{self.status.value},{self.parts},{self.max_parts},{self.completions},{self.rating},{self.start_date},{self.end_date}"

    def _precise_eq(self, other: Consumable) -> bool:
        return (
            super().__eq__(other)
            and self.series_id == other.series_id
            and self.name == other.name
            and self.type == other.type
            and self.status == other.status
            and self.parts == other.parts
            and self.max_parts == other.max_parts
            and self.completions == other.completions
            and self.rating == other.rating
            and self.start_date == other.start_date
            and self.end_date == other.end_date
        )


def average_rating(consumables: Sequence[Consumable]) -> float:
    ratings = [c.rating for c in consumables if c.rating is not None]
    if len(ratings) == 0:
        return 0.0
    else:
        return sum(ratings) / len(ratings)

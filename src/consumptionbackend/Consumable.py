# General Imports
from __future__ import annotations  # For self-referential type-hints
from typing import Union, Any
from datetime import datetime
from enum import Enum
from collections.abc import Sequence, Mapping

# Package Imports
from .Database import DatabaseEntity
from .Personnel import Personnel
from .Series import Series


class Status(Enum):
    PLANNING = 0
    IN_PROGRESS = 1
    ON_HOLD = 2
    DROPPED = 3
    COMPLETED = 4


class Consumable(DatabaseEntity):

    DB_NAME = "consumables"
    DB_PERSONNEL_MAPPING_NAME = "consumable_personnel"

    def __init__(self, *args,
                 id: Union[int, None] = None,
                 series_id: int = -1,
                 name: str = "",
                 type: str = "",
                 status: Union[Status, int] = Status.PLANNING,
                 parts: int = 0,
                 completions: int = 0,
                 rating: Union[float, None] = None,
                 start_date: Union[float, None] = None,
                 end_date: Union[float, None] = None) -> None:
        super().__init__(id)
        self.series_id = series_id
        self.name = name
        self.type = type.upper()
        self.status = status
        self.parts = parts
        self.completions = completions
        self.rating = rating
        self.start_date = start_date
        self.end_date = end_date
        self._enforce_constraints()

    def _enforce_constraints(self) -> None:
        # Conversions
        # Convert status to Enum
        if not isinstance(self.status, Status):
            self.status = Status(self.status)
        # Set completions if completed
        if self.completions == 0 and self.status == Status.COMPLETED:
            self.completions = 1
        # Change to in progress if a start_date is set
        if self.start_date is None and self.status == Status.IN_PROGRESS:
            self.start_date = datetime.utcnow().timestamp()     # Posix-timestamp
        # Parts at least 1 on COMPLETE
        if self.parts == 0 and self.status == Status.COMPLETED:
            self.parts = 1
        # Errors
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("End date must be after start date.")

    def get_personnel(self) -> Sequence[Personnel]:
        if self.id is None:
            raise ValueError(
                "Cannot find Personnel for Consumable without ID.")
        cur = self.db.cursor()
        sql = f"""SELECT * FROM {Consumable.DB_PERSONNEL_MAPPING_NAME} 
                    LEFT JOIN {Personnel.DB_NAME} 
                    ON {Consumable.DB_PERSONNEL_MAPPING_NAME}.personnel_id = {Personnel.DB_NAME}.id
                    WHERE consumable_id = ?
                """
        cur.execute(sql, [self.id])
        rows = cur.fetchall()
        personnel = []
        for row in rows:
            personnel.append(
                Personnel(id=row[3], first_name=row[4], last_name=row[5], pseudonym=row[6]))
        return personnel

    def get_series(self) -> Series:
        return Series.find(id=self.series_id)[0]

    @classmethod
    def _assert_attrs(cls, d: Mapping[str, Any]) -> None:
        attrs = {"id", "series_id", "name", "type", "status", "parts",
                 "completions", "rating", "start_date", "end_date"}
        for key in d.keys():
            if key not in attrs:
                raise ValueError(
                    f"Improper key provided in attribute mapping for Consumable: {key}")

    @classmethod
    def _seq_to_consumable(cls, seq: Sequence[Any]) -> Consumable:
        return Consumable(id=seq[0],
                          series_id=seq[1],
                          name=seq[2],
                          type=seq[3],
                          status=seq[4],
                          parts=seq[5],
                          completions=seq[6],
                          rating=seq[7],
                          start_date=seq[8],
                          end_date=seq[9]
                          )

    @classmethod
    def _consumable_to_seq(cls, cons: Consumable) -> Sequence[Any]:
        return [
            cons.id,
            cons.series_id,
            cons.name,
            cons.type,
            cons.status,
            cons.parts,
            cons.completions,
            cons.rating,
            cons.start_date,
            cons.end_date
        ]

    @classmethod
    def new(cls, **kwargs) -> Consumable:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        consumable = Consumable(**kwargs)

        sql = f"""INSERT INTO {cls.DB_NAME} 
                (id, series_id, name, type, status, parts, completions, rating, start_date, end_date)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """
        cur.execute(sql, cls._consumable_to_seq(consumable))
        cls.db.commit()
        consumable.id = cur.lastrowid
        return consumable

    @classmethod
    def find(cls, **kwargs) -> Sequence[Consumable]:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        where = []
        values = []
        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
            elif key == "type":
                where.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where.append(f"{key} = ?")
                values.append(value.value)
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"SELECT * FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        rows = cur.fetchall()
        consumables = []
        for row in rows:
            consumables.append(cls._seq_to_consumable(row))
        return consumables

    @classmethod
    def update(cls, where_map: Mapping[str, Any], set_map: Mapping[str, Any]) -> Sequence[Consumable]:
        cls._assert_attrs(where_map)
        cls._assert_attrs(set_map)
        cur = cls.db.cursor()
        values = []

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

        where_placeholders = []
        for key, value in where_map.items():
            if key == "name":
                where_placeholders.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
            elif key == "type":
                where_placeholders.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where_placeholders.append(f"{key} = ?")
                values.append(value.value)
            else:
                where_placeholders.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE {cls.DB_NAME} SET {', '.join(set_placeholders)} WHERE {' AND '.join(where_placeholders)} RETURNING *"
        cur.execute(sql, values)
        rows = cur.fetchall()
        cls.db.commit()
        consumables = []
        for row in rows:
            consumables.append(cls._seq_to_consumable(row))
        return consumables

    @classmethod
    def delete(cls, **kwargs) -> bool:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        where = []
        values = []
        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
            elif key == "type":
                where.append(f"upper({key}) = upper(?)")
                values.append(value)
            elif key == "status" and isinstance(value, Status):
                where.append(f"{key} = ?")
                values.append(value.value)
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"DELETE FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        cls.db.commit()
        return True

    def update_self(self, set_map: Mapping[str, Any]) -> Consumable:
        if self.id is None:
            raise ValueError(
                "Cannot update Consumable that does not have an ID.")
        update = self.update({"id" : self.id}, set_map)
        assert len(update) == 1
        return update[0]

    def delete_self(self) -> bool:
        if self.id is None:
            raise ValueError(
                "Cannot delete Consumable that does not have an ID.")
        return self.delete(id=self.id)

    def __str__(self) -> str:
        return f"{self.__class__.__name__} | {self.name} with ID: {self.id}"

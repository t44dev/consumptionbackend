# General Imports
from __future__ import annotations
import logging
from collections.abc import Mapping, Sequence
from typing import Union, Any

# Consumption Imports
from . import Database
from . import Consumable as cons


class Series(Database.DatabaseEntity):
    DB_NAME = "series"

    def __init__(self, *args, id: Union[int, None] = None, name: str = "") -> None:
        super().__init__(*args, id=id)
        self.name = name

    def get_consumables(self) -> Sequence[cons.Consumable]:
        if self.id is None:
            raise ValueError("Cannot find Consumables for Series without ID.")
        return cons.Consumable.find(series_id=self.id)

    @classmethod
    def _assert_attrs(cls, d: Mapping[str, Any]) -> None:
        attrs = {"id", "name"}
        for key in d.keys():
            if key not in attrs:
                raise ValueError(
                    f"Improper key provided in attribute mapping for Series: {key}"
                )

    @classmethod
    def _seq_to_series(cls, seq: Sequence[Any]) -> Series:
        return Series(id=seq[0], name=seq[1])

    @classmethod
    def new(cls, do_log: bool = True, **kwargs) -> Series:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        series = Series(**kwargs)

        sql = f"""INSERT INTO {cls.DB_NAME} 
                (id, name)
                VALUES (?,?)
            """
        cur.execute(sql, [series.id, series.name])
        cls.handler.get_db().commit()
        series.id = cur.lastrowid
        if do_log:
            logging.getLogger(__name__).info(f"NEW_SERIES#{series._csv_str()}")
        return series

    @classmethod
    def find(cls, **kwargs) -> Sequence[Series]:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        values = []
        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"SELECT * FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        rows = cur.fetchall()
        series = []
        for row in rows:
            series.append(cls._seq_to_series(row))
        return series

    @classmethod
    def update(
        cls,
        where_map: Mapping[str, Any],
        set_map: Mapping[str, Any],
        do_log: bool = True,
    ) -> Sequence[Series]:
        if len(set_map) == 0:
            raise ValueError("Set map cannot be empty.")
        cls._assert_attrs(where_map)
        cls._assert_attrs(set_map)
        old_series = {s.id: s for s in cls.find(**where_map.copy())}
        cur = cls.handler.get_db().cursor()
        values = []

        set_placeholders = []
        for key, value in set_map.items():
            set_placeholders.append(f"{key} = ?")
            values.append(value)

        where_placeholders = ["true"]
        for key, value in where_map.items():
            if key == "name":
                where_placeholders.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where_placeholders.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE {cls.DB_NAME} SET {', '.join(set_placeholders)} WHERE {' AND '.join(where_placeholders)} RETURNING *"
        cur.execute(sql, values)
        rows = cur.fetchall()
        cls.handler.get_db().commit()
        series = []
        for row in rows:
            new_ser = cls._seq_to_series(row)
            series.append(new_ser)
            if do_log:
                logging.getLogger(__name__).info(
                    f"UPDATE_SERIES#{old_series.get(new_ser.id)._csv_str()}#{new_ser._csv_str()}"
                )
        return series

    @classmethod
    def delete(cls, do_log: bool = True, **kwargs) -> bool:
        cls._assert_attrs(kwargs)
        old_series = cls.find(**kwargs.copy())
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        values = []
        for key, value in kwargs.items():
            if key == "name":
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"DELETE FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        cls.handler.get_db().commit()
        if do_log:
            logger = logging.getLogger(__name__)
            for ser in old_series:
                logger.info(f"DELETE_SERIES#{ser._csv_str()}")
        return True

    def update_self(self, set_map: Mapping[str, Any]) -> Series:
        if self.id is None:
            raise ValueError("Cannot update Series that does not have an ID.")
        update = self.update({"id": self.id}, set_map)
        assert len(update) == 1
        return update[0]

    def delete_self(self) -> bool:
        if self.id is None:
            raise ValueError("Cannot delete Series that does not have an ID.")
        return self.delete(id=self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} | {self.name} with ID: {self.id}"

    def __str__(self) -> str:
        return self.name

    def _csv_str(self) -> str:
        return f"{self.id},'{self.name}'"

    def _precise_eq(self, other: Series) -> bool:
        return super().__eq__(other) and self.name == other.name

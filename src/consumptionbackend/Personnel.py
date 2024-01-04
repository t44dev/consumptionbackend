# General Imports
from __future__ import annotations
import logging
from collections.abc import Mapping, Sequence
from typing import Union, Any

# Personnel Imports
from . import Database
from . import Consumable as cons


class Personnel(Database.DatabaseEntity):
    DB_NAME = "personnel"

    def __init__(
        self,
        *args,
        id: Union[int, None] = None,
        first_name: Union[str, None] = None,
        last_name: Union[str, None] = None,
        pseudonym: Union[str, None] = None,
        role: Union[str, None] = None,
    ) -> None:
        super().__init__(*args, id=id)
        self.first_name = first_name
        self.last_name = last_name
        self.pseudonym = pseudonym
        self.role = role

    def get_consumables(self) -> Sequence[cons.Consumable]:
        if self.id is None:
            raise ValueError("Cannot find Consumables for Personnel without ID.")
        cur = self.handler.get_db().cursor()
        sql = f"""SELECT * FROM {cons.Consumable.DB_NAME}
                    WHERE id IN
                        (
                            SELECT DISTINCT consumable_id
                            FROM {cons.Consumable.DB_PERSONNEL_MAPPING_NAME}
                            WHERE personnel_id = ?
                        )
                """
        cur.execute(sql, [self.id])
        rows = cur.fetchall()
        consumables = []
        for row in rows:
            consumables.append(cons.Consumable._seq_to_consumable(row))
        return consumables

    @classmethod
    def _assert_attrs(cls, d: Mapping[str, Any]) -> None:
        attrs = {"id", "first_name", "last_name", "pseudonym", "role"}
        for key in d.keys():
            if key not in attrs:
                raise ValueError(
                    f"Improper key provided in attribute mapping for Personnel: {key}"
                )

    @classmethod
    def _seq_to_personnel(cls, seq: Sequence[Any]) -> Personnel:
        return Personnel(
            id=seq[0], first_name=seq[1], last_name=seq[2], pseudonym=seq[3]
        )

    @classmethod
    def new(cls, do_log: bool = True, **kwargs) -> Personnel:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        personnel = Personnel(**kwargs)

        sql = f"""INSERT INTO {cls.DB_NAME}
                (id, first_name, last_name, pseudonym)
                VALUES (?,?,?,?)
            """
        cur.execute(
            sql,
            [
                personnel.id,
                personnel.first_name,
                personnel.last_name,
                personnel.pseudonym,
            ],
        )
        cls.handler.get_db().commit()
        personnel.id = cur.lastrowid
        # Logging
        if do_log:
            logging.getLogger(__name__).info(f"NEW_CONSUMABLE#{personnel._csv_str()}")
        return personnel

    @classmethod
    def find(cls, **kwargs) -> Sequence[Personnel]:
        cls._assert_attrs(kwargs)
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        values = []
        for key, value in kwargs.items():
            if key in ["first_name", "last_name", "pseudonym"]:
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"SELECT * FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        rows = cur.fetchall()
        personnel = []
        for row in rows:
            personnel.append(cls._seq_to_personnel(row))
        return personnel

    @classmethod
    def update(
        cls,
        where_map: Mapping[str, Any],
        set_map: Mapping[str, Any],
        do_log: bool = True,
    ) -> Sequence[Personnel]:
        if len(set_map) == 0:
            raise ValueError("Set map cannot be empty.")
        cls._assert_attrs(where_map)
        cls._assert_attrs(set_map)
        old_personnel = {p.id: p for p in cls.find(**where_map.copy())}
        cur = cls.handler.get_db().cursor()
        values = []

        set_placeholders = []
        for key, value in set_map.items():
            set_placeholders.append(f"{key} = ?")
            values.append(value)

        where_placeholders = ["true"]
        for key, value in where_map.items():
            if key in ["first_name", "last_name", "pseudonym"]:
                where_placeholders.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where_placeholders.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE {cls.DB_NAME} SET {', '.join(set_placeholders)} WHERE {' AND '.join(where_placeholders)} RETURNING *"
        cur.execute(sql, values)
        rows = cur.fetchall()
        cls.handler.get_db().commit()
        personnel = []
        for row in rows:
            new_pers = cls._seq_to_personnel(row)
            personnel.append(new_pers)
            # Logging
            if do_log:
                logging.getLogger(__name__).info(
                    f"UPDATE_PERSONNEL#{old_personnel.get(new_pers.id)._csv_str()}#{new_pers._csv_str()}"
                )
        return personnel

    @classmethod
    def delete(cls, do_log: bool = True, **kwargs) -> bool:
        cls._assert_attrs(kwargs)
        old_personnel = cls.find(**kwargs.copy())
        cur = cls.handler.get_db().cursor()
        where = ["true"]
        values = []
        for key, value in kwargs.items():
            if key in ["first_name", "last_name", "pseudonym"]:
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f"%{value}%")
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"DELETE FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        cls.handler.get_db().commit()
        # Logging
        if do_log:
            logger = logging.getLogger(__name__)
            for pers in old_personnel:
                logger.info(f"DELETE_PERSONNEL#{pers._csv_str()}")
        return True

    def update_self(self, set_map: Mapping[str, Any]) -> Personnel:
        if self.id is None:
            raise ValueError("Cannot update Personnel that does not have an ID.")
        update = self.update({"id": self.id}, set_map)
        assert len(update) == 1
        return update[0]

    def delete_self(self) -> bool:
        if self.id is None:
            raise ValueError("Cannot delete Personnel that does not have an ID.")
        return self.delete(id=self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} | {self.first_name} {self.last_name} with ID: {self.id}"

    def __str__(self) -> str:
        psuedonym = f'"{self.pseudonym}"' if self.pseudonym is not None else None
        name = " ".join(
            filter(
                lambda x: x is not None, [self.first_name, psuedonym, self.last_name]
            )
        )
        role = f"[{self.role}] " if self.role is not None else None
        return f"{role}{name}"

    def _csv_str(self) -> str:
        return f"{self.id},'{self.first_name}','{self.pseudonym}','{self.last_name}'"

    def _precise_eq(self, other: Personnel) -> bool:
        return (
            super().__eq__(other)
            and self.first_name == other.first_name
            and self.last_name == other.last_name
            and self.pseudonym == other.pseudonym
            and self.role == other.role
        )

    def __eq__(self, other: Personnel) -> bool:
        return super().__eq__(other) and self.role == other.role

    def __hash__(self) -> int:
        return hash(hash(super().__hash__()) + 13 * hash(self.role))

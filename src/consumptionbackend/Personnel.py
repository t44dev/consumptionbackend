# General Imports
from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import Union, Any

# Package Imports
from .Database import DatabaseEntity
from .Consumable import Consumable


class Personnel(DatabaseEntity):

    DB_NAME = "personnel"

    def __init__(self, *args,
                 id: Union[int, None] = None,
                 first_name: Union[str, None] = None,
                 last_name: Union[str, None] = None,
                 pseudonym: Union[str, None] = None,
                 role: Union[str, None] = None) -> None:
        super().__init__(id)
        self.first_name = first_name
        self.last_name = last_name
        self.pseudonym = pseudonym
        self.role = role

    def get_consumables(self) -> Sequence[Consumable]:
        if self.id is None:
            raise ValueError(
                "Cannot find Consumables for Personnel without ID.")
        cur = self.db.cursor()
        sql = f"""SELECT * FROM {Consumable.DB_NAME} 
                    WHERE consumable_id IN 
                        (
                            SELECT DISTINCT consumable_id 
                            FROM {Consumable.DB_PERSONNEL_MAPPING_NAME} 
                            WHERE personnel_id = ?
                        )
                """
        cur.execute(sql, [self.id])
        rows = cur.fetchall()
        consumables = []
        for row in rows:
            consumables.append(Consumable._seq_to_consumable(row))
        return consumables

    @classmethod
    def _assert_attrs(cls, d: Mapping[str, Any]) -> None:
        attrs = {"id", "first_name", "last_name", "pseudonym", "role"}
        for key in d.keys():
            if key not in attrs:
                raise ValueError(
                    f"Improper key provided in attribute mapping for Personnel: {key}")

    @classmethod
    def _seq_to_personnel(cls, seq: Sequence[Any]) -> Personnel:
        return Personnel(id=seq[0], first_name=seq[1], last_name=seq[2], pseudonym=seq[3])

    @classmethod
    def new(cls, **kwargs) -> Personnel:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        personnel = Personnel(**kwargs)

        sql = f"""INSERT INTO {cls.DB_NAME} 
                (id, first_name, last_name, pseudonym)
                VALUES (?,?,?,?)
            """
        cur.execute(sql, [personnel.id, personnel.first_name,
                    personnel.last_name, personnel.pseudonym])
        cls.db.commit()
        personnel.id = cur.lastrowid
        return personnel

    @classmethod
    def find(cls, **kwargs) -> Sequence[Personnel]:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        where = []
        values = []
        for key, value in kwargs.items():
            if key in ["first_name", "last_name", "pseudoynm"]:
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
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
    def update(cls, where_map: Mapping[str, Any], set_map: Mapping[str, Any]) -> Sequence[Personnel]:
        cls._assert_attrs(where_map)
        cls._assert_attrs(set_map)
        cur = cls.db.cursor()
        values = []

        set_placeholders = []
        for key, value in set_map.items():
            set_placeholders.append(f"{key} = ?")
            values.append(value)

        where_placeholders = []
        for key, value in where_map.items():
            if key in ["first_name", "last_name", "pseudoynm"]:
                where_placeholders.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
            else:
                where_placeholders.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE {cls.DB_NAME} SET {', '.join(set_placeholders)} WHERE {' AND '.join(where_placeholders)} RETURNING *"
        cur.execute(sql, values)
        rows = cur.fetchall()
        cls.db.commit()
        personnel = []
        for row in rows:
            personnel.append(cls._seq_to_personnel(row))
        return personnel

    @classmethod
    def delete(cls, **kwargs) -> bool:
        cls._assert_attrs(kwargs)
        cur = cls.db.cursor()
        where = []
        values = []
        for key, value in kwargs.items():
            if key in ["first_name", "last_name", "pseudoynm"]:
                where.append(f"upper({key}) LIKE upper(?)")
                values.append(f'%{value}%')
            else:
                where.append(f"{key} = ?")
                values.append(value)

        sql = f"DELETE FROM {cls.DB_NAME} WHERE {' AND '.join(where)}"
        cur.execute(sql, values)
        cls.db.commit()
        return True

    def update_self(self, set_map: Mapping[str, Any]) -> Personnel:
        if self.id is None:
            raise ValueError(
                "Cannot update Personnel that does not have an ID.")
        update = self.update({"id": self.id}, set_map)
        assert len(update) == 1
        return update[0]

    def delete_self(self) -> bool:
        if self.id is None:
            raise ValueError(
                "Cannot delete Personnel that does not have an ID.")
        return self.delete(id=self.id)

    def __str__(self) -> str:
        # TODO: Make this be in the format First "Pseudonym" Last, omitting NoneTypes
        return f"{self.__class__.__name__} | {self.first_name} {self.last_name} with ID: {self.id}"

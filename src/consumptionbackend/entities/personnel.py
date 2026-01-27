from dataclasses import dataclass

from .base import EntityBase


@dataclass
class Personnel(EntityBase):
    first_name: str | None
    last_name: str | None
    pseudonym: str | None

    def full_name(self) -> str:
        return " ".join(
            [
                name
                for name in [
                    self.first_name,
                    f'"{self.pseudonym}"' if self.pseudonym is not None else None,
                    self.last_name,
                ]
                if name is not None
            ]
        )

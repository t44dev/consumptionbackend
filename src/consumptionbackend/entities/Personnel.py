# stdlib
from dataclasses import dataclass
from typing import cast
from collections.abc import Sequence

# consumption
from .EntityBase import EntityBase


@dataclass
class Personnel(EntityBase):
    first_name: str | None
    last_name: str | None
    pseudonym: str | None

    def full_name(self) -> str:
        return " ".join(
            cast(
                Sequence[str],
                list(
                    filter(
                        lambda name: name is not None,
                        [
                            self.first_name,
                            (
                                f'"{self.pseudonym}"'
                                if self.pseudonym is not None
                                else None
                            ),
                            self.last_name,
                        ],
                    )
                ),
            )
        )

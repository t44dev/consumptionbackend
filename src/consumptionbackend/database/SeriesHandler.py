# stdlib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Unpack, cast

# consumption
from .database_handling import (
    ApplyMapping,
    DatabaseHandlerBase,
    EntityHandlerBase,
    WhereMapping,
)
from .fields import (
    SeriesApplyMapping,
    SeriesFieldsRequired,
)
from consumptionbackend.entities import Series


class SeriesHandlerBase(EntityHandlerBase, ABC):

    HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        return cls.HANDLER().new(Series, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Series:
        return cls.HANDLER().find_by_id(Series, id)

    @classmethod
    def find(cls, **where: Unpack[WhereMapping]) -> Sequence[Series]:
        return cls.HANDLER().find(Series, **where)

    @classmethod
    def update(
        cls,
        where: WhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[Series]:
        return cls.HANDLER().update(Series, where, cast(ApplyMapping, apply))

    @classmethod
    def delete(cls, **where: Unpack[WhereMapping]) -> None:
        return cls.HANDLER().delete(Series, **where)

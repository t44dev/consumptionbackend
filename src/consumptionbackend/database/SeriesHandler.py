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
    SeriesWhereMapping,
)
from consumptionbackend.entities import Series


class SeriesHandlerBase(EntityHandlerBase, ABC):

    _HANDLER: type[DatabaseHandlerBase]

    @classmethod
    def new(cls, **values: Unpack[SeriesFieldsRequired]) -> Series:
        return cls._HANDLER().new(Series, **values)

    @classmethod
    def find_by_id(cls, id: int) -> Series:
        return cls._HANDLER().find_by_id(Series, id)

    @classmethod
    def find(cls, **where: Unpack[SeriesWhereMapping]) -> Sequence[Series]:
        return cls._HANDLER().find(Series, **where)

    @classmethod
    def update(
        cls,
        where: SeriesWhereMapping,
        apply: SeriesApplyMapping,
    ) -> Sequence[Series]:
        return cls._HANDLER().update(
            Series, cast(WhereMapping, cast(object, where)), cast(ApplyMapping, apply)
        )

    @classmethod
    def delete(cls, **where: Unpack[SeriesWhereMapping]) -> None:
        return cls._HANDLER().delete(Series, **where)

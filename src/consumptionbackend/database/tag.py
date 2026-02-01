from abc import ABC, abstractmethod
from collections.abc import Sequence

from consumptionbackend.utils import ServiceBase


class TagService(ServiceBase, ABC):
    @abstractmethod
    def find(self) -> Sequence[str]: ...

# stdlib
from abc import ABC, abstractmethod
from typing import Any


class DatabaseProviderBase(ABC):

    @classmethod
    @abstractmethod
    def setup(cls) -> Any:
        pass

# stdlib
from abc import ABCMeta
from typing import Any, TypeVar

S = TypeVar("S", bound="Singleton")
NV = TypeVar("NV")


class Singleton(type):
    _instances: dict[type[Any], Any] = {}

    def __call__(cls: type[S], *args: Any, **kwargs: dict[str, Any]) -> S:
        if cls not in Singleton._instances:
            Singleton._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AbstractSingleton(Singleton, ABCMeta):
    _instances: dict[type[Any], Any] = {}

    def __call__(cls: type[S], *args: Any, **kwargs: dict[str, Any]) -> S:
        if cls not in AbstractSingleton._instances:
            AbstractSingleton._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]

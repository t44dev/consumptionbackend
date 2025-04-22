# stdlib
from abc import ABCMeta
from collections.abc import MutableMapping
from typing import Any, TypeVar

S = TypeVar("S", bound="SingletonMeta")


class SingletonMeta(type):
    s_instances: MutableMapping[type[Any], Any] = dict()

    def __call__(cls: type[S], *args: Any, **kwargs: dict[str, Any]) -> S:
        if cls not in SingletonMeta.s_instances:
            SingletonMeta.s_instances[cls] = super(SingletonMeta, cls).__call__(
                *args, **kwargs
            )
        return cls.s_instances[cls]


class SingletonBase:

    def __del__(self) -> None:
        if self.__class__ in SingletonMeta.s_instances:
            del SingletonMeta.s_instances[self.__class__]


class Singleton(SingletonBase, metaclass=SingletonMeta):
    pass


class AbstractSingletonMeta(SingletonMeta, ABCMeta):

    def __call__(cls: type[S], *args: Any, **kwargs: dict[str, Any]) -> S:
        if cls not in AbstractSingletonMeta.s_instances:
            SingletonMeta.s_instances[cls] = super(SingletonMeta, cls).__call__(
                *args, **kwargs
            )
        return cls.s_instances[cls]


class AbstractSingleton(SingletonBase, metaclass=AbstractSingletonMeta):
    pass

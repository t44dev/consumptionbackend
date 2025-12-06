from abc import ABCMeta
from collections.abc import MutableMapping
from typing import Any, override


class SingletonMeta(type):
    s_instances: MutableMapping[type[Any], Any] = dict()

    @override
    def __call__[S: SingletonMeta](
        cls: type[S], *args: Any, **kwargs: dict[str, Any]
    ) -> S:
        if cls not in SingletonMeta.s_instances:
            SingletonMeta.s_instances[cls] = super(SingletonMeta, cls).__call__(
                *args, **kwargs
            )
        return cls.s_instances[cls]


class SingletonBase:
    @classmethod
    def reset(cls) -> None:
        if cls in SingletonMeta.s_instances:
            del SingletonMeta.s_instances[cls]


class Singleton(SingletonBase, metaclass=SingletonMeta): ...


class AbstractSingletonMeta(SingletonMeta, ABCMeta):
    @override
    def __call__[S: SingletonMeta](
        cls: type[S], *args: Any, **kwargs: dict[str, Any]
    ) -> S:
        if cls not in AbstractSingletonMeta.s_instances:
            SingletonMeta.s_instances[cls] = super(SingletonMeta, cls).__call__(
                *args, **kwargs
            )
        return cls.s_instances[cls]


class AbstractSingleton(SingletonBase, metaclass=AbstractSingletonMeta): ...

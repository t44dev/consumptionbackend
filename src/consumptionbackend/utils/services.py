from abc import ABC
from collections.abc import MutableMapping
from typing import cast


class ServiceBase(ABC):
    pass


class ServiceProvider:
    _services: MutableMapping[type[ServiceBase], ServiceBase] = dict()

    @classmethod
    def register[S: ServiceBase](cls, key: type[S], service: S):
        cls._services[key] = service

    @classmethod
    def get[S: ServiceBase](cls, key: type[S]) -> S:
        if key not in cls._services:
            raise TypeError("No service registered of specified type.")

        return cast(S, cls._services[key])

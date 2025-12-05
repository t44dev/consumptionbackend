from typing import Any


class ConsumptionBackendException(Exception):
    def __init__(self, message: str, *args: Any) -> None:
        self.message: str = message
        super().__init__(message, args)


class NotFoundException(ConsumptionBackendException): ...


class ValidationException(ConsumptionBackendException): ...


class NoValuesException(ConsumptionBackendException): ...

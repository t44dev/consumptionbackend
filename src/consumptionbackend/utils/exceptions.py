from typing import Any


class ConsumptionBackendError(Exception):
    def __init__(self, message: str, *args: Any) -> None:
        self.message: str = message
        super().__init__(message, args)


class NotFoundError(ConsumptionBackendError): ...


class ValidationError(ConsumptionBackendError): ...


class NoValuesError(ConsumptionBackendError): ...

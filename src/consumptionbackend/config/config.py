# stdlib
import json
from pathlib import Path
from typing import TypeAlias, TypedDict, final

# 3rd party
from platformdirs import user_config_path, user_data_path

# consumption
from consumptionbackend.utils import Singleton

JSONPrimitive: TypeAlias = str | int | float | bool | None
JSONType: TypeAlias = JSONPrimitive | list["JSONType"] | dict[str, "JSONType"]


class ConfigDict(TypedDict):
    version: str
    db: str


@final
class ConsumptionConfig(metaclass=Singleton):

    CURRENT_VERSION: str = "3.0.0"
    CONFIG_DIR: Path = user_config_path("consumption")
    DATA_DIR: Path = user_data_path("consumption")

    CONFIG_FILE_PATH = CONFIG_DIR / "config.json"

    DEFAULT_CONFIG: ConfigDict = {
        "version": CURRENT_VERSION,
        "db": str(DATA_DIR / "consumption.db"),
    }

    def __init__(self) -> None:
        if not ConsumptionConfig.CONFIG_FILE_PATH.is_file():
            ConsumptionConfig.CONFIG_FILE_PATH.parent.mkdir(exist_ok=True, parents=True)
            self.__write(ConsumptionConfig.DEFAULT_CONFIG)
        self.__read()

    def write(self) -> None:
        self.__write(self._config)

    def __write(self, config: ConfigDict) -> None:
        with open(ConsumptionConfig.CONFIG_FILE_PATH, "w+") as config_file:
            json.dump(config, config_file)

    def __read(self) -> None:
        with open(ConsumptionConfig.CONFIG_FILE_PATH, "r") as config_file:
            self._config: ConfigDict = json.load(config_file)

    def __getitem__(self, key: str, value: JSONType) -> JSONType:
        return self._config[key]  # pyright:ignore[reportUnknownVariableType]

    def __setitem__(self, key: str, value: JSONType):
        self._config[key] = value

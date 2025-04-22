# stdlib
from abc import ABC, abstractmethod
import json
from pathlib import Path
from typing import TypedDict, final


class ConfigDict(TypedDict):
    version: str
    db: str


class ConfigProvider(ABC):

    @classmethod
    @abstractmethod
    def read(cls, path: Path) -> ConfigDict:
        pass

    @classmethod
    @abstractmethod
    def write(cls, path: Path, config: ConfigDict) -> None:
        pass


@final
class FileConfigProvider(ConfigProvider):
    @classmethod
    def read(cls, path: Path) -> ConfigDict:
        with open(path, "r") as config_file:
            return json.load(config_file)

    @classmethod
    def write(cls, path: Path, config: ConfigDict) -> None:
        with open(path, "w+") as config_file:
            json.dump(config, config_file)

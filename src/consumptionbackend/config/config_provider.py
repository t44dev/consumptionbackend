# stdlib
from abc import ABC, abstractmethod
import json
from pathlib import Path
from typing import TypedDict, final, override


class ConfigDict(TypedDict):
    version: str
    db: str


class ConfigProvider(ABC):

    @abstractmethod
    def setup(self) -> ConfigDict:
        pass

    @abstractmethod
    def read(self) -> ConfigDict:
        pass

    @abstractmethod
    def write(self, config: ConfigDict) -> None:
        pass


@final
class FileConfigProvider(ConfigProvider):

    def __init__(self, path: Path, default_config: ConfigDict) -> None:
        super().__init__()
        self.path = path
        self.default_config = default_config

    @override
    def setup(self) -> ConfigDict:
        if not self.path.is_file():
            self.path.parent.mkdir(exist_ok=True, parents=True)
            self.write(self.default_config)
        return self.read()

    @override
    def read(self) -> ConfigDict:
        with open(self.path, "r") as config_file:
            return json.load(config_file)

    @override
    def write(self, config: ConfigDict) -> None:
        with open(self.path, "w+") as config_file:
            json.dump(config, config_file)

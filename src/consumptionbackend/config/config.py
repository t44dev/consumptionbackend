# stdlib
from pathlib import Path
from typing import Any, final

# 3rd party
from platformdirs import user_config_path, user_data_path

# consumption
from consumptionbackend.utils import Singleton
from .config_provider import ConfigProvider, FileConfigProvider, ConfigDict


@final
class ConsumptionConfig(Singleton):

    CURRENT_VERSION: str = "3.0.0"
    CONFIG_DIR: Path = user_config_path("consumption")
    DATA_DIR: Path = user_data_path("consumption")

    CONFIG_FILE_PATH = CONFIG_DIR / "config.json"

    DEFAULT_CONFIG: ConfigDict = {
        "version": CURRENT_VERSION,
        "db": str(DATA_DIR / "consumption.db"),
    }

    _PROVIDER: ConfigProvider = FileConfigProvider(CONFIG_FILE_PATH, DEFAULT_CONFIG)

    def __init__(self) -> None:
        self._config = ConsumptionConfig._PROVIDER.setup()

    def write(self) -> None:
        self._PROVIDER.write(self._config)

    def __getitem__(self, key: str) -> Any:
        return self._config[key]  # pyright:ignore[reportUnknownVariableType]

    def __setitem__(self, key: str, value: Any):
        self._config[key] = value

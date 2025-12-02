# stdlib
from typing import final, override

# consumption
from consumptionbackend.config import ConsumptionConfig
from consumptionbackend.config.config_provider import ConfigDict, ConfigProvider


@final
class MemoryConfigProvider(ConfigProvider):

    def __init__(self) -> None:
        self.config: ConfigDict = ConsumptionConfig.DEFAULT_CONFIG

    @override
    def setup(self) -> ConfigDict:
        return self.config

    @override
    def read(self) -> ConfigDict:
        return self.config

    @override
    def write(self, config: ConfigDict) -> None:
        self.config = config

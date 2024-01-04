# Initializing config file location
from pathlib import Path
from collections.abc import Mapping
import json

DEFAULT_CONFIG = {"DB_PATH": "~/.consumption/consumption.db", "version": "2.1.0"}

CONSUMPTION_PATH = Path.home() / ".consumption"
CONFIG_PATH = CONSUMPTION_PATH / "config.json"


def setup_config() -> bool:
    if not CONFIG_PATH.is_file():
        CONSUMPTION_PATH.mkdir(exist_ok=True)
        write_config(DEFAULT_CONFIG)
        return True
    return False


def get_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def write_config(data: Mapping):
    with open(CONFIG_PATH, "w+") as f:
        json.dump(data, f)

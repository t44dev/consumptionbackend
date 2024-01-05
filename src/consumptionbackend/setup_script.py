from .config_handling import setup_config, CONSUMPTION_PATH
from .Database import DatabaseInstantiator
from .update_script import update
import logging


def setup():
    # Additional Setup
    if setup_config():
        # First Time Setup
        DatabaseInstantiator.run()
    else:
        # Config exists, check for potential updates
        update()
    # Logging
    logging.basicConfig(
        filename=CONSUMPTION_PATH / "consumption.log",
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(asctime)s#%(name)s#%(levelname)s#%(message)s",
    )

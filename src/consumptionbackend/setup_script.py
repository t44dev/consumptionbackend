from .config_handling import setup_config
from .Database import DatabaseInstantiator
from .update_script import update


def setup():
    if setup_config():
        DatabaseInstantiator.run()
    else:
        update()

from .config_handling import get_config, write_config
from .Database import DatabaseInstantiator, DatabaseHandler


def update():
    config = get_config()
    version = config.get("version", "0.0.0").split(".")
    if version != ["2", "1", "0"]:
        # Update Config
        config["version"] = "2.1.0"
        write_config(config)

    if version[0] == "1":
        # Update Existing DB
        cur = DatabaseHandler.get_db().cursor()
        script = """
            ALTER TABLE consumables RENAME TO
                consumables_old;
        """
        cur.executescript(script)
        DatabaseInstantiator.run()
        script2 = """
            INSERT INTO consumables 
                SELECT id, -1, name, type, status, minor_parts, major_parts, completions, rating, start_date, end_date
                FROM consumables_old;
            DROP TABLE consumables_old;
            INSERT INTO personnel 
                SELECT id, first_name, last_name, pseudonym
                FROM staff;
            DROP TABLE staff;
        """
        cur.executescript(script2)

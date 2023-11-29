from .config_handling import get_config, write_config
from .Database import DatabaseInstantiator, DatabaseHandler

def update():
    config = get_config()
    if "version" not in config or config["version"] != "2.0.0":
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
        # Update Config
        config["version"] = "2.0.0"
        write_config(config)

BEGIN TRANSACTION;
-- Versioning
CREATE table version (
    major INTEGER,
    minor INTEGER,
    patch INTEGER
);

INSERT INTO 
    version (major, minor, patch)
VALUES
    (3, 0, 0);

-- Setup Main Tables
-- Series
CREATE TABLE series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);

INSERT INTO
    series (id, name)
VALUES
    (- 1, 'None');

INSERT INTO
    sqlite_sequence (name, seq)
VALUES
    ('series', 0);

-- Personnel
CREATE TABLE personnel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    last_name TEXT,
    pseudonym TEXT
);

-- Consumables
CREATE TABLE consumables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL DEFAULT - 1,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    parts INTEGER NOT NULL DEFAULT 0,
    max_parts INTEGER DEFAULT NULL,
    completions INTEGER NOT NULL DEFAULT 0,
    rating REAL,
    start_date REAL,
    end_date REAL,
    FOREIGN KEY (series_id) REFERENCES series (id)
        ON DELETE SET DEFAULT
        ON UPDATE NO ACTION
);

-- Mapping Tables
CREATE TABLE consumable_personnel (
    personnel_id INTEGER NOT NULL,
    consumable_id INTEGER NOT NULL,
    role TEXT,
    PRIMARY KEY (personnel_id, consumable_id, role)
    FOREIGN KEY (personnel_id) REFERENCES personnel (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
    FOREIGN KEY (consumable_id) REFERENCES consumables (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);

CREATE TABLE consumable_tags (
    consumable_id INTEGER NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (consumable_id, tag)
    FOREIGN KEY (consumable_id) REFERENCES consumables (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);

-- Add Triggers
-- Rules Triggers
CREATE TRIGGER completions_on_completed_update
    AFTER UPDATE ON consumables FOR EACH ROW
    WHEN NEW.status = 4 AND NEW.completions = OLD.completions AND NEW.status != OLD.status
BEGIN
    UPDATE
        consumables
    SET
        completions = completions + 1
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER completions_on_completed_insert
    AFTER INSERT ON consumables
    WHEN NEW.completions = 0 AND NEW.status = 4
BEGIN
    UPDATE
        consumables
    SET
        completions = 1
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER start_date_on_in_progress_update
    AFTER UPDATE ON consumables FOR EACH ROW
    WHEN NEW.start_date IS NULL AND NEW.status > 0
BEGIN
    UPDATE
        consumables
    SET
        start_date = strftime('%s')
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER start_date_on_in_progress_insert
    AFTER INSERT ON consumables
    WHEN NEW.start_date IS NULL AND NEW.status > 0
BEGIN
    UPDATE
        consumables
    SET
        start_date = strftime('%s')
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER end_date_on_completed_update
    AFTER UPDATE ON consumables FOR EACH ROW
    WHEN NEW.end_date IS NULL AND NEW.status = 4
BEGIN
    UPDATE
        consumables
    SET
        end_date = max(strftime('%s'), NEW.start_date)
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER end_date_on_completed_insert
    AFTER INSERT ON consumables
    WHEN NEW.end_date IS NULL AND NEW.status = 4
BEGIN
    UPDATE
        consumables
    SET
        end_date = max(strftime('%s'), NEW.start_date)
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER parts_on_completed_update
    AFTER UPDATE ON consumables FOR EACH ROW
    WHEN NEW.status = 4 AND OLD.status != 4
BEGIN
    UPDATE
        consumables
    SET
        parts = max(NEW.parts, coalesce(NEW.max_parts, 1)),
        max_parts = max(NEW.parts, coalesce(NEW.max_parts, 1))
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER parts_on_completed_insert
    AFTER INSERT ON consumables
    WHEN NEW.status = 4
BEGIN
    UPDATE
        consumables
    SET
        parts = max(NEW.parts, coalesce(NEW.max_parts, 1)),
        max_parts = max(NEW.parts, coalesce(NEW.max_parts, 1))
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER upper_type_update
    AFTER UPDATE ON consumables FOR EACH ROW
    WHEN NEW.type != OLD.type
BEGIN
    UPDATE
        consumables
    SET
        type = upper(NEW.type)
    WHERE
        id = NEW.id;
END;

CREATE TRIGGER upper_type_insert
    AFTER INSERT ON consumables
BEGIN
    UPDATE
        consumables
    SET
        type = upper(NEW.type)
    WHERE
        id = NEW.id;
END;

-- Error Triggers
CREATE TRIGGER date_error_update
    AFTER UPDATE ON consumables
    WHEN NEW.start_date IS NOT NULL AND NEW.end_date IS NOT NULL AND NEW.start_date > NEW.end_date
BEGIN
    SELECT RAISE(ROLLBACK, 'end date must be after start date');
END;

CREATE TRIGGER date_error_insert
    AFTER INSERT ON consumables
    WHEN NEW.start_date IS NOT NULL AND NEW.end_date IS NOT NULL AND NEW.start_date > NEW.end_date
BEGIN
    SELECT RAISE (ROLLBACK, 'end date must be after start date');
END;

CREATE TRIGGER delete_none_series BEFORE
    DELETE ON series FOR EACH ROW
    WHEN OLD.id = -1
BEGIN
    SELECT RAISE (ROLLBACK, 'cannot delete series with ID -1');
END;

COMMIT;

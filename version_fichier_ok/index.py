# function file for tar multithreading index using sqlite3 db
import sqlite3
import os


class index:
    conn = None
    name = None

    def __init__(self, name):
        self.name = name
        self.connect_database()

    def create_database(self):
        self.conn = sqlite3.connect(self.name, timeout=30, check_same_thread=False)

        sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                                id integer PRIMARY KEY,
                                                name text NOT NULL,
                                                start_offset NUMERIC DEFAULT 0,
                                                size NUMERIC DEFAULT 0
                                            ); """

        c = self.conn.cursor()
        c.execute(sql_create_projects_table)
        self.conn.commit()

    def connect_database(self):
        if os.path.isfile(self.name):
            self.conn = sqlite3.connect(self.name, timeout=30, check_same_thread=False)
        else:
            self.create_database()

        self.conn.execute("PRAGMA journal_mode=WAL")

    # Insert must be in one transaction to use serialized multithreading from sqlite
    def insert_and_retrieve_new_offset(self, size_of_tar, name_tar_to_add):
        sql_insert_and_retrieve_new_offset = """
BEGIN TRANSACTION;

INSERT INTO projects
VALUES (NULL, "{1}", (SELECT CASE WHEN ((SELECT COUNT(*) FROM projects LIMIT 1) = 0) THEN 0 ELSE (SELECT (start_offset + size) FROM projects WHERE id = (SELECT MAX(id) FROM projects)) END), {0});

COMMIT;
        """.format(size_of_tar, name_tar_to_add)

        c = self.conn.cursor()
        c.executescript(sql_insert_and_retrieve_new_offset)
        self.conn.commit()

        sql_retrieve_start_offset = """
        SELECT start_offset FROM projects WHERE name = '{0}';
""".format(name_tar_to_add)

        return c.execute(sql_retrieve_start_offset).fetchone()[0]

    def close(self):
        self.conn.close()

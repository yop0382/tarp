# function file for tar multithreading index using sqlite3 db
import psycopg
import os


class index:
    conn = None
    name = None

    def __init__(self, name):
        self.name = name
        self.connect_database()

    def create_table(self):
        sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS {0} (
                                                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                name text NOT NULL,
                                                start_offset bigint DEFAULT 0,
                                                size bigint DEFAULT 0
                                            ); """.format(self.name)

        c = self.conn.cursor()
        c.execute(sql_create_projects_table)
        c.execute("""set enable_parallel_hash=off;""")
        self.conn.commit()

    def connect_database(self):
        self.conn = psycopg.connect("host=localhost dbname=projects user=postgres password=postgres")

        # c = self.conn.cursor()
        # c.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{0}')".format(self.name))
        # r = c.fetchone()[0]
        # print(r)
        # if not r:
        try:
            self.create_table()
        except (Exception):
            self.connect_database()

    # Insert must be in one transaction to use serialized multithreading from sqlite
    def insert_and_retrieve_new_offset(self, size_of_tar, name_tar_to_add):
        sql_insert_and_retrieve_new_offset = """
INSERT INTO {2} (name, start_offset, size)
VALUES (
'{1}', 
(SELECT COALESCE((SELECT (start_offset + size) FROM {2} WHERE id = (SELECT MAX(id) FROM {2})),0))
, {0});
        """.format(size_of_tar, name_tar_to_add, self.name)

        # print(sql_insert_and_retrieve_new_offset)

        c = self.conn.cursor()
        c.execute(sql_insert_and_retrieve_new_offset)
        self.conn.commit()

        sql_retrieve_start_offset = """
        SELECT start_offset FROM {1} WHERE name = '{0}';
""".format(name_tar_to_add, self.name)

        return c.execute(sql_retrieve_start_offset).fetchone()[0]

    def close(self):
        self.conn.close()

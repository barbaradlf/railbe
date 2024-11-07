import duckdb


class RailDB():
    def __init__(self, dbname):
        self.conn = duckdb.connect(dbname)

    def save_table(self, file, tablename):
       self.conn.sql(f"CREATE TABLE {tablename} AS SELECT * FROM read_parquet('{file}')")

    def close(self):
        self.conn.close()

import duckdb
import os
import time
from railbe.irail import RailRequest
from pathlib import Path



class RailDB():
    def __init__(self, dbname, dir = "data"):
        self.outdir = Path(dir)
        if not os.path.exists(self.outdir):
            os.mkdir(self.outdir)
        self.dbpath = Path(dir, dbname).with_suffix(".db")
        self.conn = duckdb.connect(self.dbpath)

    def _save_table(self, file, tablename, append = False, update = False):
        if tablename not in self.tables:
            self.conn.sql(f"CREATE TABLE {tablename} AS SELECT * FROM read_parquet('{file}')")
        elif append:
            self.conn.sql(f"INSERT INTO {tablename} SELECT * FROM read_parquet('{file}')")
        elif update:
            self.conn.sql(f"DROP TABLE {tablename}")
            self.conn.sql(f"CREATE TABLE {tablename} AS SELECT * FROM read_parquet('{file}')")
        else:
            raise ValueError(f"Table {tablename} already exists. Use update = True to overwrite or append = True to add data.")

    @property
    def tables(self):
        tables = self.conn.sql("SHOW TABLES;").fetchall()
        return [table[0] for table in tables]

    def update_stations(self):
        endpoint = "stations"
        filename = f"{endpoint}.parquet"
        stations = RailRequest(endpoint)
        stations.df.to_parquet(filename)
        self._save_table(file = filename,
                        tablename = endpoint,
                        update = True)
        print(f"Saved table with {len(stations.df)} stations to database.")

    @property
    def stationids(self):
        try:
            station_ids = self.conn.sql("SELECT id FROM stations;").fetchall()
        except duckdb.CatalogException:
            raise AttributeError("Table stations not available. Run `update_stations()` first.")
        return [station_id[0] for station_id in station_ids]

    def update_liveboard(self):
        self._empty_liveboard()
        for stationid in self.stationids:
            endpoint = "liveboard"
            filename = Path(self.outdir, f"{endpoint}_{stationid}").with_suffix(".parquet")
            lb = RailRequest(endpoint, id = stationid)
            lb.get_response()
            if lb.response.status_code == 429:
                time.sleep(10)
            lb.df.to_parquet(filename)
            self._save_table(file = filename,
                            tablename = "liveboard",
                            append = True)
            print(f"Updating liveboard for station {stationid}")

    def _empty_liveboard(self):
        try:
            self.conn.sql("DROP TABLE liveboard;")
        except duckdb.CatalogException:
            pass

    def close(self):
        self.conn.close()

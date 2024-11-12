import duckdb
import os
from railbe.irail import RailRequest
from pathlib import Path
import pandas as pd



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
        filename = Path(self.outdir, f"{endpoint}").with_suffix(".parquet")
        stations = RailRequest(endpoint, with_wait = False)
        stations.df.to_parquet(filename)
        self._save_table(file = filename,
                        tablename = endpoint,
                        update = True)
        print(f"Saved table with {len(stations.df)} stations to database.")

    @property
    def stationids(self):
        if not "stations" in self.tables:
            raise AttributeError("Table stations not available. Run `update_stations()` first.")
        station_ids = self.conn.sql("SELECT id FROM stations;").fetchall()
        station_ids = [station_id[0] for station_id in station_ids]
        station_names = self.conn.sql("SELECT name FROM stations;").fetchall()
        station_names = [station_name[0] for station_name in station_names]
        return dict(zip(station_ids, station_names))

    def update_liveboard(self, stationids = None, append = False):
        if not append:
            self._empty_liveboard()
        if stationids is None:
            stationids = self.stationids
        endpoint = "liveboard"
        for stationid in stationids:
            stationname = self.stationids[stationid]
            filename = Path(self.outdir, f"{endpoint}_{stationid}.").with_suffix(".parquet")
            print(f"Updating liveboard for station {stationname}...")
            lb = RailRequest(endpoint, id = stationid, with_wait = True)
            stationdata = lb.df
            stationdata["stationid"] = stationid
            if stationdata.shape[0] > 0:
                stationdata.to_parquet(filename)
                self._save_table(file = filename,
                                tablename = "liveboard",
                                append = True)
                print(f"SUCCESS! Liveboard retrieved for station {stationname}.")
            else:
                print(f"Liveboard retrieved for {stationname}, but no data was available.")

    def _empty_liveboard(self):
        try:
            self.conn.sql("DROP TABLE liveboard;")
        except duckdb.CatalogException:
            pass

    def close(self):
        self.conn.close()

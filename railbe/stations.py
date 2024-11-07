from railbe.irail import RailRequest
from railbe.raildb import RailDB
import tempfile

class Stations(RailRequest):
    ENDPOINT = "stations"
    SUFFIX = "?format=json&lang=nl"
    STATIONKEY = "station"

    @property
    def url(self):
        if not hasattr(self, "_url"):
            self._url = f"{self.API}/{self.VERSION}/{self.ENDPOINT}{self.SUFFIX}"
        return self._url

    @property
    def stationdf(self):
        return self.get_df(self.STATIONKEY)

    def create_db(self):
        # save df as parquet file
        tmpfilename = tempfile.mktemp()
        print(tmpfilename)
        self.save_parquet(key = self.STATIONKEY, filename = tmpfilename)

        self.db = RailDB(dbname = "stations.db")
        self.db.save_table(file = tmpfilename, tablename = self.STATIONKEY)
        self.db.close()


if __name__ == "__main__":
    stations = Stations()
    stations.create_db()
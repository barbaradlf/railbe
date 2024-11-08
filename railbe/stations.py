from railbe.irail import RailRequest

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

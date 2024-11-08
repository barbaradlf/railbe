import pytest
from railbe.stations import Stations
import pandas as pd

def test_station():
    stations = Stations(application = "tests")
    station_data = stations.response.json()
    assert type(station_data) == dict
    assert "station" in station_data.keys()
    assert stations.STATIONKEY in station_data.keys()

    assert type(stations.stationdf) == pd.DataFrame
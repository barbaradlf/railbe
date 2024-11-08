import pytest
from railbe.irail import RailRequest
import pandas as pd


@pytest.mark.parametrize("endpoint, key, id", [
    ("stations", "station", None),
    ("liveboard", "departures", "BE.NMBS.008812005")])
def test_request(endpoint, key, id):
    request = RailRequest(endpoint = endpoint, id = id, application = "tests")
    data = request.response.json()
    assert type(data) == dict
    assert key in data.keys()
    assert request.KEY in data.keys()
    assert type(request.df) == pd.DataFrame

import pytest
from railbe.irail import RailRequest
import pandas as pd


@pytest.mark.parametrize("endpoint, id", [
    ("stations", None),
    ("liveboard", "BE.NMBS.008812005")])
def test_request(endpoint, id):
    request = RailRequest(endpoint = endpoint, id = id, application = "tests")
    assert type(request.df) == pd.DataFrame

def test_liveboard():
    with pytest.raises(ValueError, match = "For liveboard requests, you must provide a stationid."):
        RailRequest(endpoint = "liveboard")

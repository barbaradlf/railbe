import requests
from warnings import warn
import pandas as pd

class RailRequest():
    API = "https://api.irail.be"
    VERSION = "v1"
    SUFFIX = "format=json&lang=nl"
    ENDPOINT = {
        "stations": "stations",
        "liveboard": "liveboard"
    }
    KEY = {
        "stations": "station",
        "liveboard": "departures.departure"
    }

    def __init__(self,
                 endpoint: str,
                 id: str = None,
                 application: str = "demo",
                 website: str = "https://www.datalab.nl",
                 email: str = "barbara@datalab.nl"):
        self.headers = {
            "user-agent": f"{application} ({website}; {email})"
        }
        if endpoint not in self.ENDPOINT.keys():
            raise ValueError(f"Endpoint must be one of {self.ENDPOINT.keys()}.")
        if endpoint == "liveboard" and not id:
            raise ValueError("For liveboard requests, you must provide a stationid.")
        if id:
            self.id = f"id={id}"
        else:
            self.id = ""
        self.ENDPOINT = self.ENDPOINT[endpoint]
        self.KEY = self.KEY[endpoint]

    @property
    def url(self):
        if not hasattr(self, "_url"):
            self._url = f"{self.API}/{self.VERSION}/{self.ENDPOINT}/?{self.id}&{self.SUFFIX}"
        return self._url

    def get_response(self):
        response = requests.get(self.url, headers=self.headers)
        if response.status_code != 200:
            warn(f"Request failed with status code {response.status_code}. Response is not updated.")
        else:
            print(f"Successfully retrieved data from {self.url}")
            self._response = response

    @property
    def response(self):
        if not hasattr(self, "_response"):
            self.get_response()
        if hasattr(self, "_response"):
            return self._response
        else:
            raise AttributeError("Response not available.")

    def get_df(self):
        data = self.response.json()
        key = self.KEY.split(".")
        for key in key:
            data = data[key]
        return pd.json_normalize(data)

    @property
    def df(self):
        return self.get_df()

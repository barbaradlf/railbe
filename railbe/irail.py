import requests
from warnings import warn
import pandas as pd
import time

class RailRequest():
    API = "https://api.irail.be"
    VERSION = "v1"
    SUFFIX = "format=json&lang=nl"
    KEY = {
        "stations": "station",
        "liveboard": "departures.departure"
    }
    RATE = 3 # max requests per second

    def __init__(self,
                 endpoint: str,
                 id: str = None,
                 application: str = "demo",
                 website: str = "https://www.datalab.nl",
                 email: str = "barbara@datalab.nl",
                 with_wait: bool = True):
        self.headers = {
            "user-agent": f"{application} ({website}; {email})"
        }
        try:
            self.KEY = self.KEY[endpoint]
        except KeyError:
            raise ValueError(f"Endpoint must be one of {self.KEY.keys()}.")
        if endpoint == "liveboard" and not id:
            raise ValueError("For liveboard requests, you must provide a stationid.")
        if id:
            self.id = f"id={id}"
        else:
            self.id = ""
        self.endpoint = endpoint
        # the rate limit is RATE requests per second. To prevent 429 errors
        # we can opt to pre-emptively wait before making the request.
        # if with_wait is False, the wait time is set to 0.
        self._preemptive_wait = with_wait * (1. / self.RATE)

    @property
    def url(self):
        if not hasattr(self, "_url"):
            self._url = f"{self.API}/{self.VERSION}/{self.endpoint}/?{self.id}&{self.SUFFIX}"
        return self._url

    def get_response(self):
        '''
        Retrieve data from the API and store it in the _response attribute.
        '''
        time.sleep(self._preemptive_wait)
        status = 429
        count_429 = 0
        while status == 429 and count_429 < 5:
            response = requests.get(self.url, headers=self.headers)
            status = response.status_code
            if status == 429:
                warn("Rate limit exceeded. Waiting 2 seconds before retrying.")
                time.sleep(2)
                count_429 += 1
        if response.status_code != 200:
            warn(f"Request failed with status code {response.status_code}. Response is not updated.")
        else:
            print(f"Successfully retrieved data from {self.url}")
            # update response object only if the status code is 200 (OK)
            self._response = response

    @property
    def response(self):
        if not hasattr(self, "_response"):
            self.get_response()
        if hasattr(self, "_response"):
            return self._response
        else:
            raise AttributeError("Response not available.")

    @property
    def df(self):
        data = self.response.json()
        keysplit = self.KEY.split(".")
        for key in keysplit:
            data = data[key] # data is updated with subkeys
        return pd.json_normalize(data)

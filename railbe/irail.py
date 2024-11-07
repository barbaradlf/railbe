import abc
import requests
from warnings import warn
import pandas as pd

class RailRequest(abc.ABC):
    API = "https://api.irail.be"
    VERSION = "v1"
    def __init__(self,
                 application: str = "demo",
                 website: str = "https://www.datalab.nl",
                 email: str = "barbara@datalab.nl"):
        self.headers = {
            "user-agent": f"{application} ({website}; {email})"
        }

    @property
    def url(self):
        return NotImplemented

    def get_response(self):
        response = requests.get(self.url, headers=self.headers)
        if response.status_code != 200:
            warn(f"Request failed with status code {response.status_code}. Response is not updated.")
        else:
            self._response = response

    @property
    def response(self):
        if not hasattr(self, "_response"):
            self.get_response()
        if hasattr(self, "_response"):
            return self._response
        else:
            raise AttributeError("Response not available.")

    def get_df(self, key: str = None):
        data = self.response.json()
        if key:
            data = data[key]
        return pd.json_normalize(data)

    def save_parquet(self, key: str = None, filename: str = None):
        df = self.get_df(key)
        df.to_parquet(filename)

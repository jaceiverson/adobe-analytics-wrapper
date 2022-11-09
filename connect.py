import os
import datetime as dt
from enum import Enum
from typing import Union, Optional

import requests
import jwt
import pandas as pd
from rich.console import Console

from util import read_json

# Load env variables
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "https://analytics.adobe.io/"
JWT_URL = "https://ims-na1.adobelogin.com/ims/exchange/jwt"


class RequestType(Enum):
    GET = 0
    POST = 1


class AdobeAnalytics:
    def __init__(self, rsid: str = None, console: Optional[Console] = None) -> None:
        """on init, will get access token using a jwt payload"""
        self.RSID = rsid
        if console:
            self.console = console
        else:
            self.console = Console()

        self.ACCESS_TOKEN = self._get_access_token_()
        self.BASE_REPORTING_URL = self._get_global_id_()

    def _get_access_token_(self, expiration_date: dt.datetime = None):
        """Returns the access token required for auth with adobe apis"""
        # if no expiration date (set one for 1 day in the future)
        # if there is one, convert it to a timestamp
        if expiration_date is None:
            expiration_date = int(
                (dt.datetime.now() + dt.timedelta(days=1)).timestamp()
            )
        else:
            expiration_date = int(expiration_date.timestamp())

        # CREATE JWT PAYLOAD
        # https://github.com/AdobeDocs/adobeio-auth/blob/stage/JWT/JWT.md#required-claims-for-a-service-account-jwt
        jwt_payload = {
            "exp": int((dt.datetime.now() + dt.timedelta(days=1)).timestamp()),
            "iss": os.environ["ORG_ID"],
            "sub": os.environ["TECH_ID"],
            "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": True,
            "aud": f"https://ims-na1.adobelogin.com/c/{os.environ['CLIENT_ID']}",
        }
        # read in our private key
        with open("./creds/keys/private.key", "r") as f:
            PRIVATE_KEY = f.read()
        # ENCODE JWT
        encoded_jwt = jwt.encode(jwt_payload, PRIVATE_KEY, algorithm="RS256")
        # EXCHANGE JWT for ACCESS TOKEN
        # https://github.com/AdobeDocs/adobeio-auth/blob/stage/JWT/JWT.md#exchanging-jwt-to-retrieve-an-access-token
        access_payload = {
            "client_id": os.environ["CLIENT_ID"],
            "client_secret": os.environ["CLIENT_SECRET"],
            "jwt_token": encoded_jwt,
        }
        # MAKE THE REQUEST
        access_token_response = requests.post(JWT_URL, data=access_payload)
        # if we get 200, return the access token,
        if access_token_response.status_code == 200:
            return access_token_response.json()["access_token"]

    def _get_global_id_(self):
        self.global_details = self.make_request(
            "discovery/me", BASE_URL, self.make_header(global_id=True)
        )
        self.GLOBAL_CO_ID = self.global_details["imsOrgs"][0]["companies"][0][
            "globalCompanyId"
        ]
        return f"https://analytics.adobe.io/api/{self.GLOBAL_CO_ID}/"

    def from_workspace(self, workspace_json: Union[dict, str]) -> dict:
        # if it is a string (file path) we will read the json file
        if isinstance(workspace_json, str):
            workspace_json = read_json(workspace_json)

        metric_count = workspace_json["metricContainer"].get("metrics")

        self.console.log(
            f"Pulling API from workspace JSON for {len(metric_count) if metric_count else None } metrics",
        )
        response = requests.post(
            url=f"{self.BASE_REPORTING_URL}reports",
            headers=self.make_header(),
            json=workspace_json,
        )
        if str(response.status_code).startswith("2"):
            return response.json()

    def make_header(self, additional_header: dict = None, global_id: bool = False):
        """Adds values to the base header if passed in"""
        base_header = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "x-api-key": os.environ["CLIENT_ID"],
        }
        # if we are looking for the global id, this is all we need
        if global_id:
            return base_header
        # if not, we will need to add the global company id in
        base_header.update({"x-proxy-global-company-id": self.GLOBAL_CO_ID})
        # then we can add any additional items we may want
        if isinstance(additional_header, dict):
            base_header.update(additional_header)
        return base_header

    def make_request(
        self,
        endpoint: str,
        url: str = None,
        request_header: dict = None,
        request_type: RequestType = "GET",
        post_body: dict = None,
    ):
        """Accepts an endpoint, makes the request, returns the json response"""
        # if no header, pull in the default one
        if request_header is None:
            request_header = self.make_header()
        if url is None:
            url = self.BASE_REPORTING_URL
        # Add in the RSID
        if "?" in endpoint:
            endpoint += f"&rsid={self.RSID}"
        else:
            endpoint += f"?rsid={self.RSID}"

        if request_type == "GET":
            self.console.log(f"Pulling: {url+endpoint}")
            response = requests.get(
                url + endpoint,
                headers=request_header,
            )
            self.recent_response = response

        elif request_type == "POST" and post_body:
            self.console.log(f"Posting: {url+endpoint}")
            response = requests.post(
                url + endpoint, headers=request_header, json=post_body
            )
            self.recent_response = response

        return response.json()

    def set_date_range(
        self,
        start: dt.date = dt.date.today() - dt.timedelta(days=7),
        end: dt.date = dt.date.today(),
    ):
        """
        sets the start and end dates and returns a formated string of the dates
        this formated version is used in the report body
        """
        self.start_date = start
        self.end_date = end
        return self.__format_date_range__()

    def __format_date_range__(self):
        """
        returns formated date range as follows
        start_dateT00:00:00/end_dateT00:00:00
        setting each day to midnight
        """
        return f"{self.start_date.strftime('%Y-%m-%d')}T00:00:00.000/{self.end_date.strftime('%Y-%m-%d')}T00:00:00.000"

    def get_report_suite_id(self, limit: int = 10, page: int = 0):
        endpoint = f"/collections/suites?limit={limit}&page={page}"
        self.reporting_suites = self.make_request(endpoint)

    def get_segments(self, limit: int = 10, page: int = 0):
        return self.make_request(f"segments?limit={limit}")

    def get_dimensions(self, limit: int = 10):
        return self.make_request(f"dimensions?limit={limit}")

    def get_metrics(self, limit: int = 10):
        return self.make_request(f"metrics?limit={limit}")

    def get_projects(self, limit: int = 10, page: int = 0):
        endpoint = f"project?&limit={limit}&page={page}"
        return self.make_request(endpoint)

    def get_report(self, metrics_list, dimension, segment_id):
        """
        Makes the report body for a request
        You must have already called self.set_date_range()
        """
        report_body = {
            "rsid": self.RSID,
            "globalFilters": [
                {
                    "type": "dateRange",
                    "dateRange": self.__format_date_range__(),
                    "segmentId": segment_id,
                }
            ],
            "metricContainer": {
                "metrics": [
                    {"columnId": idx, "id": x} for idx, x in enumerate(metrics_list)
                ],
            },
            "dimension": dimension,
            "settings": {
                "dimensionSort": "asc",
                "limit": 50000,
            },
        }
        return self.make_request("reports", request_type="POST", post_body=report_body)

    def _get_metric_names(self, json_body: dict) -> list:
        """retuns a list of metric names"""
        return [
            x["id"].split("/")[1] if "/" in x["id"] else f"{idx}-{x['id']}"
            for idx, x in enumerate(json_body["metricContainer"]["metrics"])
        ]

    def _parse_output(self, report_response: dict) -> pd.DataFrame:
        pass


if __name__ == "__main__":
    pass

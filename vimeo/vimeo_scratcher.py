#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for Vimeo
data.
This scratcher does not use the PyVimeo interface due to lack of documentation
and attempt to maintain exponential backoff, as the VimeoClient from PyVimeo
cannot be mounted with exponential backoff adapter.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import query_secrets
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
ACCESS_TOKEN = query_secrets.ACCESS_TOKEN
CLIENT_ID = query_secrets.CLIENT_ID
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_vimeo_{today.year}_{today.month}_{today.day}.csv"
)


def get_license_list():
    """Provides the list of license from a Creative Commons searched licenses.
    Returns:
        List: A list containing all license types that should be searched in
        all possible license filters of Vimeo API.
    """
    return [
        "CC",
        "CC-BY",
        "CC-BY-NC",
        "CC-BY-NC-ND",
        "CC-BY-NC-SA",
        "CC-BY-ND",
        "CC-BY-SA",
        "CC0",
    ]


def get_request_url(license="CC"):
    """Provides the API Endpoint URL for specified parameter combinations.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    return (
        f"https://api.vimeo.com/videos?filter={license}"
        f"&client_id={CLIENT_ID}&access_token={ACCESS_TOKEN}"
    )


def get_response_elems(license):
    """Provides the metadata for query of specified parameters
    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        request_url = get_request_url(license=license)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        return {"totalResults": search_data["total"]}
    except Exception as e:
        raise e


def set_up_data_file():
    """Writes the header row to file to contain Vimeo data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{header_title}\n")


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to file to contain Vimeo Query data.
    Args:
        license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    """
    data_log = (
        f"{license_type},"
        f"{get_response_elems(license_type)['totalResults']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    license_list = get_license_list()
    for license_type in license_list:
        record_license_data(license_type)


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("INFO (130) Halted via KeyboardInterrupt.", file=sys.stderr)
        sys.exit(130)
    except Exception:
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
        sys.exit(1)

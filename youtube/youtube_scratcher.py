"""
This file is dedicated to obtain a .csv record report for Youtube
Data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import query_secret
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
API_KEY = query_secret.API_KEY
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_youtube_{today.year}_{today.month}_{today.day}.csv"
)


def get_request_url():
    """Provides the API Endpoint URL for specified parameter combinations.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    return (
        r"https://youtube.googleapis.com/youtube/v3/search?part=snippet"
        r"&type=video&videoLicense=creativeCommon"
        f"&key={API_KEY}"
    )


def get_response_elems():
    """Provides the metadata for query of specified parameters
    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    search_data = None
    try:
        request_url = get_request_url()
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
        return search_data
    except Exception:
        if "pageInfo" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain metmuseum data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{header_title}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_wRITE_FILE as specified in that constant.
    """
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(
            "licenses/by/3.0,"
            f"{get_response_elems()['pageInfo']['totalResults']}\n"
        )


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

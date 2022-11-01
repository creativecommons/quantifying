#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for DeviantArt
data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import pandas as pd
import query_secrets
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
API_KEYS = query_secrets.API_KEYS
API_KEYS_IND = 0
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_deviantart_{today.year}_{today.month}_{today.day}.csv"
)
PSE_KEY = query_secrets.PSE_KEY


def get_license_list():
    """Provides the list of license from 2018's record of Creative Commons.
    Returns:
        np.array: An np array containing all license types that should be
        searched via Programmable Search Engine.
    """
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    return license_list[4:]


def get_request_url(license):
    """Provides the API Endpoint URL for specified parameter combinations.
    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description.
    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    try:
        api_key = API_KEYS[API_KEYS_IND]
        return (
            "https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}"
            "&q=_&relatedSite=deviantart.com"
            f'&linkSite=creativecommons.org{license.replace("/", "%2F")}'
        )
    except Exception as e:
        if isinstance(e, IndexError):
            print("Depleted all API Keys provided", file=sys.stderr)
        else:
            raise e


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
        request_url = get_request_url(license)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 500, 502, 503, 504],
            # 429 is Quota Limit Exceeded, which will be handled alternatively
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        search_data_dict = {
            "totalResults": search_data["searchInformation"]["totalResults"]
        }
        return search_data_dict
    except Exception as e:
        if isinstance(e, requests.exceptions.HTTPError):
            global API_KEYS_IND
            API_KEYS_IND += 1
            print(
                "Changing API KEYS due to depletion of quota", file=sys.stderr
            )
            return get_response_elems(license)
        else:
            raise e


def set_up_data_file():
    """Writes the header row to file to contain DeviantArt data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{header_title}\n")


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to file to contain DeviantArt data.
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

#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for MetMuseum Search
data.
"""

# Standard library
import datetime as dt
import logging
import os
import sys

# Third-party
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_metmuseum_{today.year}_{today.month}_{today.day}.csv"
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_request_url():
    """Provides the API Endpoint URL for specified parameter combinations.
    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    return "https://collectionapi.metmuseum.org/public/collection/v1/objects"


def get_response_elems():
    """Provides the metadata for query of specified parameters

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
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
    except Exception as e:
        if "pageInfo" not in search_data:
            logger.error(f"Search data is: \n{search_data}")
            sys.exit(1)
        else:
            logger.error(f"Error occurred during request: {e}")
            raise e


def set_up_data_file():
    """Writes the header row to file to contain metmuseum data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"publicdomain/zero/1.0,{get_response_elems()['total']}\n")


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        logger.exception("Unhandled exception:")
        sys.exit(1)

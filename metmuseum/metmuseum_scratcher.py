#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for MetMuseum Search
data.
"""

# Standard library
import datetime as dt
import os
import sys
import logging

# Third-party
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_metmuseum_{today.year}_{today.month}_{today.day}.csv"
)

# Set up the logger
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Define both the handler and the formatter
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

# Add formatter to the handler
handler.setFormatter(formatter)

# Add handler to the logger
LOG.addHandler(handler)

# Log the start of the script execution
LOG.info("Script execution started.")

def get_request_url():
    """Provides the API Endpoint URL for specified parameter combinations.
    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    LOG.info("Providing the API Endpoint URL for specified parameter combinations.")
    
    return "https://collectionapi.metmuseum.org/public/collection/v1/objects"


def get_response_elems():
    """Provides the metadata for query of specified parameters

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    LOG.info("Providing the metadata for query of specified parameters.")
    
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
            LOG.exception(f"search data is: \n{search_data}")
            sys.exit(1)
        else:
            raise e


def set_up_data_file():
    """Writes the header row to file to contain metmuseum data."""
    LOG.info("Writing the header row to file to contain metmuseum data.")
    
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    LOG.info("Recording the data of all license types in the license list and recording them into DATA_WRITE_FILE")
    
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"publicdomain/zero/1.0,{get_response_elems()['total']}\n")


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    # Exception Handling
    try:
        main()
    except SystemExit as e:
        LOG.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOG.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOG.error(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)

#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for MetMuseum Search
data.
"""

# Standard library
import os
import sys

# Third-party
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# First-party/Local
import quantify

# Setup paths, Date and LOGGER using quantify.setup()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
PATH_REPO_ROOT, PATH_WORK_DIR, PATH_DOTENV, DATETIME_TODAY, LOGGER = (
    quantify.setup(__file__)
)

# Set up file path for CSV report
DATA_WRITE_FILE = (
    f"{PATH_WORK_DIR}"
    f"/data_metmuseum_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv"
)


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
            LOGGER.error(f"Search data is: \n{search_data}")
            sys.exit(1)
        else:
            LOGGER.error(f"Error occurred during request: {e}")
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
        LOGGER.error("System exit with code: %d", e.code)
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception("Unhandled exception:")
        sys.exit(1)

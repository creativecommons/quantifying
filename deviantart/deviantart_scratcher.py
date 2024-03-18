#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for DeviantArt
data.
"""

# Standard library
import datetime as dt
import os
import sys
# import traceback - no longer in use after logging
import logging

# Third-party
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up current working directory
CWD = os.path.dirname(os.path.abspath(__file__))
# Load environment variables
dotenv_path = os.path.join(os.path.dirname(CWD), ".env")
load_dotenv(dotenv_path)

# Get the current date
today = dt.datetime.today()
# Retrieve API keys
API_KEYS = os.getenv("GOOGLE_API_KEYS").split(",")
API_KEYS_IND = 0
# Set up file path for CSV report
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_deviantart_{today.year}_{today.month}_{today.day}.csv"
)
# Retrieve Programmable Search Engine key from environment variables
PSE_KEY = os.getenv("PSE_KEY")

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

def get_license_list():
    """
    Provides the list of license from 2018's record of Creative Commons.

    Returns:
    - np.array: An array containing all license types that should be
    searched via Programmable Search Engine.
    """
    LOG.info("Retrieving list of license from Creative Commons' record.")

    # Read license data from file
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    # Define regex pattern to extract license types
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    return license_list[4:]


def get_request_url(license):
    """
    Provides the API Endpoint URL for specified parameter combinations.
    Args:
    - license (str): A string representing the type of license. It's a
    segment of the URL towards the license description.

    Returns:
    - str: The API Endpoint URL for the query specified by parameters.
    """
    LOG.info("Generating API Endpoint URL for specified license: %s", license)
    
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
            LOG.exception("Depleted all API Keys provided")
        else:
            raise e


def get_response_elems(license):
    """
    Provides the metadata for query of specified parameters
    Args:
    - license (str): A string representing the type of license.
    It's a segment of the URL towards the license description. If not provided,
    it defaults to None, indicating no assumption about the license type.

    Returns:
    - dict: A dictionary mapping metadata to its value provided from the API
    query.
    """
    LOG.info("Making a request to the API and handling potential retries.")
    
    try:
        # Make a request to the API and handle potential retries
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
            # If quota limit exceeded, switch to the next API key
            global API_KEYS_IND
            API_KEYS_IND += 1
            LOG.exception("Changing API KEYS due to depletion of quota")
            return get_response_elems(license)
        else:
            raise e


def set_up_data_file():
    """Writes the header row to the file to contain DeviantArt data."""
    LOG.info("Setting up data file by writing the header row.")
    
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to the file to contain DeviantArt data.
    Args:
    - license_type(str): A string representing the type of license.
    It's a segment of the URL towards the license description. If not provided,
    it defaults to None, indicating no assumption about the license type.
    """
    LOG.info("Writing the row for license type %s to contain DeviantArt data", license_type)
    
    data_log = (
        f"{license_type},"
        f"{get_response_elems(license_type)['totalResults']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def record_all_licenses():
    """
    Records the data for all available license types listed in the license
    list and writes this data into the DATA_WRITE_FILE, as specified by the
    constant.
    """
    LOG.info("Recording data for all available license types.")
    
    # Get the list of license types
    license_list = get_license_list()
    # Record data for each license types
    for license_type in license_list:
        record_license_data(license_type)


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOG.error("System exit with code: %d", e.code)
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOG.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOG.exception("Unhandled exception occurred during script execution:")
        sys.exit(1)

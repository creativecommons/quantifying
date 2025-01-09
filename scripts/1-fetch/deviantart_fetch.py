#!/usr/bin/env python
"""
This file is dedicated to querying data from the DeviantArt API.
"""

# Standard library
import argparse
import csv
import os
import sys
import traceback

# Third-party
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Global Variable for API_KEYS indexing
API_KEYS_IND = 0

# Gets API_KEYS and PSE_KEY from .env file
API_KEYS = os.getenv("GOOGLE_API_KEYS").split(",")
PSE_KEY = os.getenv("PSE_KEY")

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(
        description="DeviantArt Data Fetching Script"
    )
    parser.add_argument(
        "--licenses", type=int, default=10, help="Number of licenses to query"
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE TYPE,Document Count\n"
    with open(
        os.path.join(PATHS["data_phase"], "deviantart_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_license_list():
    """
    Provides the list of licenses from Creative Commons.

    Returns:
        list: A list containing all license types that should be searched.
    """
    LOGGER.info("Retrieving list of licenses from Creative Commons' record.")
    cc_license_data = pd.read_csv(
        os.path.join(PATHS["repo"], "legal-tool-paths.txt"), header=None
    )
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    return license_list


def get_request_url(license_type):
    """
    Provides the API Endpoint URL for a specified license type.

    Args:
        license_type: A string representing the type of license.

    Returns:
        str: The API Endpoint URL for the query specified by parameters.
    """
    LOGGER.info(f"Generating API Endpoint URL for license: {license_type}")
    try:
        api_key = API_KEYS[API_KEYS_IND]
        return (
            "https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}"
            "&q=_&relatedSite=deviantart.com"
            f'&linkSite=creativecommons.org{license_type.replace("/", "%2F")}'
        )
    except IndexError:
        LOGGER.error("Depleted all API Keys provided")
        raise shared.QuantifyingException("No API keys left to use", 1)


def get_response_elems(license_type):
    """
    Retrieves the number of documents for the specified license type.

    Args:
        license_type: A string representing the type of license.

    Returns:
        dict: A dictionary containing the total document count.
    """
    LOGGER.info(f"Querying metadata for license: {license_type}")
    try:
        request_url = get_request_url(license_type)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 500, 502, 503, 504],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        return {
            "totalResults": search_data["searchInformation"]["totalResults"]
        }
    except requests.exceptions.HTTPError as e:
        global API_KEYS_IND
        API_KEYS_IND += 1
        LOGGER.error(f"HTTP Error: {e}. Switching to the next API key.")
        if API_KEYS_IND < len(API_KEYS):
            return get_response_elems(license_type)
        else:
            raise shared.QuantifyingException(
                f"HTTP Error: {e}. No API keys left.", 1
            )
    except requests.RequestException as e:
        LOGGER.error(f"Request Exception: {e}")
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        LOGGER.error(f"KeyError: {e}.")
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def retrieve_license_data(args):
    """
    Retrieves the data of all license types specified.

    Args:
        args: Parsed command-line arguments.

    Returns:
        int: The total number of documents retrieved.
    """
    LOGGER.info("Retrieving the data for all license types.")
    licenses = get_license_list()[: args.licenses]

    # data = []
    total_docs_retrieved = 0

    for license_type in licenses:
        data_dict = get_response_elems(license_type)
        total_docs_retrieved += int(data_dict["totalResults"])
        record_results(license_type, data_dict)

    return total_docs_retrieved


def record_results(license_type, data):
    """
    Records the data for a specific license type into the CSV file.

    Args:
        license_type: The license type.
        data: A dictionary containing the data to record.
    """
    LOGGER.info(f"Recording data for license: {license_type}")
    row = [license_type, data["totalResults"]]
    with open(
        os.path.join(PATHS["data_phase"], "deviantart_fetched.csv"),
        "a",
        newline="",
    ) as f:
        writer = csv.writer(f, dialect="unix")
        writer.writerow(row)


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.

    Returns:
        dict: The last recorded state.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved (deviantart)": 0}


def save_state(state: dict):
    """
    Saves the state to a YAML file.

    Args:
        state: The state dictionary to save.
    """
    with open(PATHS["state"], "w") as f:
        yaml.safe_dump(state, f)


def main():

    # Fetch and merge changes
    shared.fetch_and_merge(PATHS["repo"])

    args = parse_arguments()

    state = load_state()
    total_docs_retrieved = state["total_records_retrieved (deviantart)"]
    LOGGER.info(f"Initial total_records_retrieved: {total_docs_retrieved}")
    goal_documents = 1000  # Set goal number of documents

    if total_docs_retrieved >= goal_documents:
        LOGGER.info(
            f"Goal of {goal_documents} documents already achieved."
            " No further action required."
        )
        return

    # Log the paths being used
    shared.paths_log(LOGGER, PATHS)

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if total_docs_retrieved == 0:
        set_up_data_file()

    # Retrieve and record data
    docs_retrieved = retrieve_license_data(args)

    # Update the state with the new count of retrieved records
    total_docs_retrieved += docs_retrieved
    LOGGER.info(
        f"Total documents retrieved after fetching: {total_docs_retrieved}"
    )
    state["total_records_retrieved (deviantart)"] = total_docs_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_quarter"], "Add and commit DeviantArt data"
    )

    # Push changes
    shared.push_changes(PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)

#!/usr/bin/env python
"""
This file is dedicated to querying data from the MetMuseum API.
"""

# Standard library
import argparse
import csv
import os
import sys
import traceback

# Third-party
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(
        description="MetMuseum Data Fetching Script"
    )
    parser.add_argument(
        "--licenses", type=int, default=1, help="Number of licenses to query"
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE TYPE,Document Count\n"
    with open(
        os.path.join(PATHS["data_phase"], "metmuseum_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_request_url():
    """
    Provides the API Endpoint URL for MetMuseum data.

    Returns:
        string: The API Endpoint URL for the query.
    """
    LOGGER.info("Providing the API Endpoint URL for MetMuseum data.")
    return "https://collectionapi.metmuseum.org/public/collection/v1/objects"


def get_response_elems():
    """
    Retrieves the total number of documents from the MetMuseum API.

    Returns:
        dict: A dictionary containing the total document count.
    """
    LOGGER.info("Querying metadata from the MetMuseum API.")
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
        return {"totalResults": search_data.get("total", 0)}
    except Exception as e:
        LOGGER.error(f"Error occurred during request: {e}")
        raise shared.QuantifyingException(f"Error fetching data: {e}", 1)


def retrieve_license_data():
    """
    Retrieves the data for the public domain license from the MetMuseum API.

    Returns:
        int: The total number of documents retrieved.
    """
    LOGGER.info(
        "Retrieving the data for public domain license from MetMuseum."
    )
    data_dict = get_response_elems()
    total_docs_retrieved = int(data_dict["totalResults"])
    record_results("publicdomain/zero/1.0", data_dict)
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
        os.path.join(PATHS["data_phase"], "metmuseum_fetched.csv"),
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
    return {"total_records_retrieved (metmuseum)": 0}


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

    # args = parse_arguments()

    state = load_state()
    total_docs_retrieved = state["total_records_retrieved (metmusuem)"]
    LOGGER.info(f"Initial total_documents_retrieved: {total_docs_retrieved}")
    goal_documents = 1000  # Set goal number of documents

    if total_docs_retrieved >= goal_documents:
        LOGGER.info(
            f"Goal of {goal_documents} documents already achieved."
            " No further action required."
        )
        return

    # Log the paths being used
    shared.log_paths(LOGGER, PATHS)

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if total_docs_retrieved == 0:
        set_up_data_file()

    # Retrieve and record data
    docs_retrieved = retrieve_license_data()

    # Update the state with the new count of retrieved records
    total_docs_retrieved += docs_retrieved
    LOGGER.info(
        f"Total documents retrieved after fetching: {total_docs_retrieved}"
    )
    state["total_records_retrieved (metmuseum)"] = total_docs_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_phase"], "Add and commit MetMuseum data"
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

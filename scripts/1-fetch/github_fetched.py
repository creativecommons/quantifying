#!/usr/bin/env python
"""
This file is dedicated to querying data from the GitHub API.
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
    parser = argparse.ArgumentParser(description="GitHub Data Fetching Script")
    parser.add_argument(
        "--licenses", type=int, default=3, help="Number of licenses to query"
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE_TYPE,Repository Count\n"
    with open(
        os.path.join(PATHS["data_phase"], "github_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_response_elems(license_type):
    """
    Provides the metadata for a query of
    specified license type from GitHub API.

    Args:
        license_type: A string representing the type of license.
    Returns:
        dict: A dictionary mapping metadata
        to its value provided from the API query.
    """
    LOGGER.info(f"Querying metadata for license: {license_type}")
    try:
        base_url = "https://api.github.com/search/repositories?q=license:"
        request_url = f"{base_url}{license_type}"
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
        return {"totalResults": search_data["total_count"]}
    except requests.HTTPError as e:
        LOGGER.error(f"HTTP Error: {e}")
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        LOGGER.error(f"Request Exception: {e}")
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        LOGGER.error(f"KeyError: {e}.")
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def retrieve_license_data(args):
    """
    Retrieves the data of all license types specified.
    """
    LOGGER.info("Retrieving the data for all license types.")
    licenses = ["CC0-1.0", "CC-BY-4.0", "CC-BY-SA-4.0"][: args.licenses]

    data = []
    total_repos_retrieved = 0

    for license_type in licenses:
        data_dict = get_response_elems(license_type)
        total_repos_retrieved += data_dict["totalResults"]
        record_results(license_type, data_dict)

    for row in data:
        LOGGER.info(f"Collected data row: {row}")

    return data


def record_results(license_type, data):
    """
    Records the data for a specific license type into the CSV file.
    """
    LOGGER.info(f"Recording data for license: {license_type}")
    row = [license_type, data["totalResults"]]
    with open(
        os.path.join(PATHS["data_phase"], "github_fetched.csv"),
        "a",
        newline="",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(row)


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved (github)": 0}


def save_state(state: dict):
    """
    Saves the state to a YAML file.
    Parameters:
        state_file: Path to the state file.
        state: The state dictionary to save.
    """
    with open(PATHS["state"], "w") as f:
        yaml.safe_dump(state, f)


def main():

    # Fetch and merge changes
    shared.fetch_and_merge(PATHS["repo"])

    args = parse_arguments()

    state = load_state()
    total_records_retrieved = state["total_records_retrieved (github)"]
    LOGGER.info(f"Initial total_records_retrieved: {total_records_retrieved}")
    goal_records = 1000  # Set goal number of records

    if total_records_retrieved >= goal_records:
        LOGGER.info(
            f"Goal of {goal_records} records already achieved."
            " No further action required."
        )
        return

    # Log the paths being used
    shared.log_paths(LOGGER, PATHS)

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if total_records_retrieved == 0:
        set_up_data_file()

    # Retrieve and record data
    repos_retrieved = retrieve_license_data(args)

    # Update the state with the new count of retrieved records
    total_records_retrieved += repos_retrieved
    LOGGER.info(
        f"Total records retrieved after fetching: {total_records_retrieved}"
    )
    state["total_records_retrieved (github)"] = total_records_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(PATHS["repo"], "Added and committed GitHub data")

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

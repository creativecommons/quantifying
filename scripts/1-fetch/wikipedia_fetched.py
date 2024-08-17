#!/usr/bin/env python
"""
This file is dedicated to querying data from the Wikipedia API.
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
        description="Wikipedia Data Fetching Script"
    )
    parser.add_argument(
        "--languages",
        type=str,
        nargs="+",
        default=["en"],
        help="List of Wikipedia language codes to query",
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = (
        "language,articles,edits,images,"
        "users,activeusers,admins,jobqueue,views\n"
    )
    with open(
        os.path.join(PATHS["data_phase"], "wikipedia_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_request_url(lang="en"):
    """
    Provides the API Endpoint URL for specified parameter combinations.

    Args:
        lang: A string representing the language for the Wikipedia API.

    Returns:
        string: The API Endpoint URL for the query.
    """
    LOGGER.info(f"Generating request URL for language: {lang}")
    base_url = (
        r"https://{lang}.wikipedia.org/w/api.php"
        "?action=query&meta=siteinfo"
        "&siprop=statistics&format=json"
    )
    return base_url.format(lang=lang)


def get_response_elems(language="en"):
    """
    Provides the metadata for query of specified parameters.

    Args:
        language: A string representing the language for the Wikipedia API.

    Returns:
        dict: A dictionary mapping metadata
        to its value provided from the API query.
    """
    LOGGER.info(f"Querying Wikipedia API for language: {language}")
    try:
        request_url = get_request_url(language)
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
        stats = search_data.get("query", {}).get("statistics", {})
        stats["language"] = language
        return stats
    except Exception as e:
        LOGGER.error(f"Error occurred during API request: {e}")
        raise shared.QuantifyingException(f"Error fetching data: {e}", 1)


def record_results(stats):
    """
    Records the data for a specific language into the CSV file.

    Args:
        stats: A dictionary of Wikipedia statistics.
    """
    LOGGER.info(f"Recording data for language: {stats.get('language')}")
    row = [
        stats.get("language", ""),
        stats.get("articles", 0),
        stats.get("edits", 0),
        stats.get("images", 0),
        stats.get("users", 0),
        stats.get("activeusers", 0),
        stats.get("admins", 0),
        stats.get("jobqueue", 0),
        stats.get("views", 0),
    ]
    with open(
        os.path.join(PATHS["data_phase"], "wikipedia_fetched.csv"),
        "a",
        newline="",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(row)


def retrieve_and_record_data(args):
    """
    Retrieves and records the data for all specified languages.
    """
    LOGGER.info("Starting data retrieval and recording.")
    total_records_retrieved = 0

    for lang in args.languages:
        stats = get_response_elems(lang)
        if stats:
            record_results(stats)
            total_records_retrieved += 1

    return total_records_retrieved


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved (wikipedia)": 0}


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
    total_records_retrieved = state["total_records_retrieved (wikipedia)"]
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
    records_retrieved = retrieve_and_record_data(args)

    # Update the state with the new count of retrieved records
    total_records_retrieved += records_retrieved
    LOGGER.info(
        f"Total records retrieved after fetching: {total_records_retrieved}"
    )
    state["total_records_retrieved (wikipedia)"] = total_records_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(PATHS["repo"], "Added and committed Wikipedia data")

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

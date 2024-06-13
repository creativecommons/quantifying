#!/usr/bin/env python
"""
This file is dedicated to querying data from the Google Custom Search API.
"""
# Standard library
import argparse
import json
import os
import sys

# import time
import traceback
from typing import List

# Third-party
import googleapiclient.discovery
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.errors import HttpError

# Setup paths and LOGGER using shared library
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# sys.path.append(".")

# First-party/Local
import shared  # noqa: E402

(
    PATH_REPO_ROOT,
    PATH_WORK_DIR,
    PATH_DOTENV,
    DATETIME_TODAY,
    LOGGER,
) = shared.setup(__file__)

# Assign DATETIME_TODAY from shared library to TODAY
TODAY = DATETIME_TODAY.date()

# Load environment variables
load_dotenv(PATH_DOTENV)

# Constants
API_KEY = os.getenv("GOOGLE_API_KEYS")
CX = os.getenv("CUSTOM_SEARCH_ENGINE_ID")
BASE_URL = "https://www.googleapis.com/customsearch/v1"
STATE_FILE = os.path.join(PATH_WORK_DIR, "state.json")

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_search_service():
    """
    Creates and returns the Google Custom Search API service.
    """
    return googleapiclient.discovery.build(
        "customsearch", "v1", developerKey=API_KEY
    )


def fetch_results(args, start_index: int) -> (List[dict], int):
    """
    Fetch search results from Google Custom Search API.
    Returns a list of seach items.
    """
    query = args.query
    records_per_query = args.records
    pages = args.pages

    service = get_search_service()
    all_results = []

    for page in range(pages):
        try:
            results = (
                service.cse()
                .list(q=query, cx=CX, num=records_per_query, start=start_index)
                .execute()
            )
            all_results.extend(results.get("items", []))
            start_index += records_per_query
        except HttpError as e:
            LOGGER.error(f"Error fetching results: {e}")
            break

    return all_results, start_index


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Google Custom Search Script")
    parser.add_argument(
        "--query", type=str, required=True, help="Search query"
    )
    parser.add_argument(
        "--records", type=int, default=1, help="Number of records per query"
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to query"
    )
    return parser.parse_args()


# State Management


def load_state(state_file: str):
    """
    Loads the state from a JSON file, returns the last fetched start index.
    """
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    return {"start_index": 1}


def save_state(state_file: str, state: dict):
    """
    Saves the state to a JSON file.
    Parameters:
        state_file: Path to the state file.
        start_index: Last fetched start index.
    """
    with open(state_file, "w") as f:
        json.dump(state, f)


def main():

    args = parse_arguments()
    state = load_state(STATE_FILE)
    start_index = state["start_index"]
    all_results, start_index = fetch_results(args, start_index)

    LOGGER.info(f"PATH_REPO_ROOT: {PATH_REPO_ROOT}")

    # Create new directory structure for year and quarter
    quarter = pd.PeriodIndex([TODAY], freq="Q")[0]
    quarter_str = str(quarter)
    data_directory = os.path.join(
        PATH_REPO_ROOT, "data", "1-fetched", quarter_str
    )
    os.makedirs(data_directory, exist_ok=True)

    # Convert results to DataFrame to save as CSV
    df = pd.DataFrame(all_results)
    file_name = os.path.join(data_directory, "gcs_fetched.csv")

    df.to_csv(file_name, index=False)

    # Save the state checkpoint after fetching
    state["start_index"] = start_index
    save_state(STATE_FILE, state)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)

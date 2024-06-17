#!/usr/bin/env python
"""
This file is dedicated to querying data from the Google Custom Search API.
"""
# Standard library
import argparse
import json
import os
import re
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


def fetch_results(
    args, start_index: int, cr=None, lr=None
) -> (List[dict], int):
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
                .list(
                    q=query,
                    cx=CX,
                    num=records_per_query,
                    start=start_index,
                    cr=cr,
                    lr=lr,
                )
                .execute()
            )
            return results.get("searchInformation", {}).get("totalResults", 0)
        except HttpError as e:
            LOGGER.error(f"Error fetching results: {e}")
            return 0

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


def set_up_data_file(data_directory):
    """
    Sets up the data files for recording results.
    Results are currently grouped by location (country) and language
    """
    header = (
        "LICENSE TYPE,No Priori,Australia,Brazil,Canada,Egypt,"
        "Germany,India,Japan,Spain,"
        "United Kingdom,United States,Arabic,"
        "Chinese (Simplified),Chinese (Traditional),"
        "English,French,Indonesian,Portuguese,Spanish\n"
    )
    # open 'w' = open a file for writing
    with open(os.path.join(data_directory, "gcs_fetched.csv"), "w") as f:
        f.write(header)


def record_results(data_directory, results):
    """ """
    # open 'a' = Open for appending at the end of the file without truncating
    with open(os.path.join(data_directory, "gcs_fetched.csv"), "a") as f:
        for result in results:
            f.write(",".join(str(value) for value in result) + "\n")


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


def get_license_list():
    """
    Provides the list of licenses from Creative Commons.

    Returns:
    - np.array:
            An np array containing all license types that should be searched
            via Programmable Search Engine (PSE).
    """
    license_list = []
    with open(
        os.path.join(PATH_REPO_ROOT, "legal-tool-paths.txt"), "r"
    ) as file:
        for line in file:
            match = re.search(r"((?:[^/]+/){2}(?:[^/]+)).*", line)
            if match:
                license_list.append(match.group(1))
    return list(set(license_list))  # Remove duplicates


def get_country_list(select_all=False):
    """
    Provides the list of countries to find Creative Commons usage data on.
    """
    countries = []
    with open(
        os.path.join(PATH_REPO_ROOT, "google_countries.tsv"), "r"
    ) as file:
        for line in file:
            country = line.strip().split("\t")[0]
            country = country.replace(",", " ")
            countries.append(country)

    if select_all:
        return sorted(countries)

    selected_countries = [
        "India",
        "Japan",
        "United States",
        "Canada",
        "Brazil",
        "Germany",
        "United Kingdom",
        "Spain",
        "Australia",
        "Egypt",
    ]
    return sorted(
        [country for country in countries if country in selected_countries]
    )


def get_lang_list():
    """
    Provides the list of languages to find Creative Commons usage data on.
    """
    languages = []
    with open(os.path.join(PATH_REPO_ROOT, "google_lang.txt"), "r") as file:
        for line in file:
            match = re.search(r'"([^"]+)"', line)
            if match:
                languages.append(match.group(1))

    selected_languages = [
        "Arabic",
        "Chinese (Simplified)",
        "Chinese (Traditional)",
        "English",
        "French",
        "Indonesian",
        "Portuguese",
        "Spanish",
    ]
    return sorted([lang for lang in languages if lang in selected_languages])


def record_license_data(args, license_list, data_directory):
    """
    Records the data of all license types into the CSV file.
    """
    selected_countries = get_country_list()
    selected_languages = get_lang_list()

    data = []

    for license_type in license_list:
        row = [license_type]
        no_priori_search = fetch_results(args, start_index=1)
        row.append(no_priori_search)

        for country in selected_countries:
            country_data = fetch_results(
                args, start_index=1, cr=f"country{country}"
            )
            row.append(country_data)

        for language in selected_languages:
            language_data = fetch_results(
                args, start_index=1, lr=f"lang_{language}"
            )
            row.append(language_data)

        data.append(row)

    record_results(data_directory, data)


def main():

    args = parse_arguments()
    state = load_state(STATE_FILE)
    start_index = state["start_index"]

    LOGGER.info(f"PATH_REPO_ROOT: {PATH_REPO_ROOT}")
    LOGGER.info(f"PATH_WORK_DIR: {PATH_WORK_DIR}")

    # Create new directory structure for year and quarter
    quarter = pd.PeriodIndex([TODAY], freq="Q")[0]
    quarter_str = str(quarter)
    data_directory = os.path.join(
        PATH_REPO_ROOT, "data", "1-fetched", quarter_str
    )
    os.makedirs(data_directory, exist_ok=True)

    set_up_data_file(data_directory)

    license_list = get_license_list()

    record_license_data(args, license_list, data_directory)

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

#!/usr/bin/env python
"""
This file is dedicated to querying data from the YouTube API.
"""

# Standard library
import argparse
import csv
import datetime
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

# Load environment variables
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(
        description="YouTube Data Fetching Script"
    )
    parser.add_argument(
        "--license_type",
        type=str,
        default="licenses/by/3.0",
        help="License type to query",
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE TYPE,Time,Document Count\n"
    with open(
        os.path.join(PATHS["data_phase"], "youtube_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_next_time_search_interval():
    """
    Provides the next searching interval of time
    for Creative Commons licensed video.

    Yields:
        tuple: A tuple representing the time search interval currently
        dealt via 2 RFC 3339 formatted date-time values (by YouTube
        API Standards), and current starting year/month of the interval.
    """
    LOGGER.info("Generating time intervals for search.")
    datetime_today = datetime.datetime.today()
    cur_year, cur_month = 2009, 1
    while (
        cur_year * 100 + cur_month
        <= datetime_today.year * 100 + datetime_today.month
    ):
        end_month, end_day = 12, 31
        if cur_month == 1:
            end_month, end_day = 2, 28 + int(cur_year % 4 == 0)
        elif cur_month == 3:
            end_month, end_day = 4, 30
        elif cur_month == 5:
            end_month, end_day = 6, 30
        elif cur_month == 7:
            end_month, end_day = 8, 31
        elif cur_month == 9:
            end_month, end_day = 10, 31
        elif cur_month == 11:
            end_month, end_day = 12, 31
        yield (
            f"{cur_year}-{cur_month:02d}-01T00:00:00Z",
            f"{cur_year}-{end_month:02d}-{end_day:02d}T23:59:59Z",
            cur_year,
            cur_month,
        )
        cur_month += 2
        if cur_month > 12:
            cur_month = 1
            cur_year += 1


def get_request_url(time=None):
    """
    Provides the API Endpoint URL for specified parameter combinations.

    Args:
        time: A tuple indicating the time interval for the query.

    Returns:
        string: The API Endpoint URL for the query.
    """
    LOGGER.info("Generating request URL for time interval.")
    base_url = (
        r"https://youtube.googleapis.com/youtube/v3/search?"
        "part=snippet&type=video&videoLicense=creativeCommon"
    )
    if time is not None:
        base_url += f"&publishedAfter={time[0]}&publishedBefore={time[1]}"
    return f"{base_url}&key={API_KEY}"


def get_response_elems(time=None):
    """
    Provides the metadata for query of specified parameters.

    Args:
        time: A tuple indicating the time interval for the query.

    Returns:
        dict: A dictionary mapping metadata to
        its value provided from the API query.
    """
    LOGGER.info(f"Querying YouTube API for time interval: {time[2]}-{time[3]}")
    try:
        request_url = get_request_url(time=time)
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
        return search_data.get("pageInfo", {}).get("totalResults", 0)
    except Exception as e:
        LOGGER.error(f"Error occurred during API request: {e}")
        raise shared.QuantifyingException(f"Error fetching data: {e}", 1)


def record_results(license_type, time, document_count):
    """
    Records the data for a specific license type
    and time interval into the CSV file.

    Args:
        license_type: The license type.
        time: The time interval.
        document_count: The number of documents.
    """
    LOGGER.info(
        f"Recording data for license: {license_type},"
        "time: {time}, count: {document_count}"
    )
    row = [license_type, time, document_count]
    with open(
        os.path.join(PATHS["data_phase"], "youtube_fetched.csv"),
        "a",
        newline="",
    ) as f:
        writer = csv.writer(f, dialect="unix")
        writer.writerow(row)


def retrieve_and_record_data(args):
    """
    Retrieves and records the data for all license types and time intervals.
    """
    LOGGER.info("Starting data retrieval and recording.")
    total_documents_retrieved = 0

    for time in get_next_time_search_interval():
        document_count = get_response_elems(time=time)
        record_results(
            args.license_type, f"{time[2]}-{time[3]}", document_count
        )
        total_documents_retrieved += document_count

    return total_documents_retrieved


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved (youtube)": 0}


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
    total_docs_retrieved = state["total_records_retrieved (youtube)"]
    LOGGER.info(f"Initial total_documents_retrieved: {total_docs_retrieved}")
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
    docs_retrieved = retrieve_and_record_data(args)

    # Update the state with the new count of retrieved records
    total_docs_retrieved += docs_retrieved
    LOGGER.info(
        f"Total documents retrieved after fetching: {total_docs_retrieved}"
    )
    state["total_records_retrieved (youtube)"] = total_docs_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_quarter"], "Add and commit YouTube data"
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

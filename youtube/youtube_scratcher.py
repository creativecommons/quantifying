#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for Youtube Search
Data.
"""

# Standard library
import os
import sys
import traceback

# Third-party
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup paths, Date and LOGGER using quantify.setup()
_, PATH_WORK_DIR, PATH_DOTENV, DATETIME_TODAY, LOGGER = quantify.setup(
    __file__
)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Get the YouTube API key
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Set up file path for CSV report
DATA_WRITE_FILE = os.path.join(
    PATH_WORK_DIR,
    "data_youtube_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)
DATA_WRITE_FILE_TIME = os.path.join(
    PATH_WORK_DIR,
    "data_youtube_time_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_next_time_search_interval():
    """
    Provides the next searching interval of time for Creative Commons
    licensed video.

    Yields:
    - tuple: A tuple representing the time search interval currently dealt
    via 2 RFC 3339 formatted date-time values (by YouTube API Standards),
    and the current starting year and month of the interval.
    """
    LOGGER.info(
        "Providing the next searching interval "
        "of time for Creative Commons licensed video."
    )

    cur_year, cur_month = 2009, 1
    while (
        cur_year * 100 + cur_month
        <= DATETIME_TODAY.year * 100 + DATETIME_TODAY.month
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
            f"{cur_year}-{cur_month}-01T00:00:00Z",
            f"{cur_year}-{end_month}-{end_day}T23:59:59Z",
            cur_year,
            cur_month,
        )
        cur_month = (cur_month + 2) % 12
        if cur_month == 1:
            cur_year += 1


def get_request_url(time=None):
    """
    Provides the API Endpoint URL for specified parameter combinations.

    Args:
    - time: A tuple indicating whether this query is related to video time
    occerrence, and the time interval which it would like to investigate.
    Defaults to None to indicate the query is not related to video time
    occurrence.

    Returns:
    - string: A string representing the API Endpoint URL for the query
    specified by this function's parameters.
    """
    LOGGER.info(
        "Providing the API Endpoint URL for specified parameter combinations."
    )

    base_url = (
        r"https://youtube.googleapis.com/youtube/v3/search?part=snippet"
        r"&type=video&videoLicense=creativeCommon&"
    )
    if time is not None:
        base_url = (
            f"{base_url}&"
            f"publishedAfter={time[0]}&"
            f"publishedBefore={time[1]}&"
        )
    return f"{base_url}key={API_KEY}"


def get_response_elems(time=None):
    """
    Provides the metadata for query of specified parameters

    Args:
    - time: A tuple indicating whether this query is related to video time
    occurrence, and the time interval which it would like to investigate.
    Defaults to None to indicate the query is not related to video time
    occurrence.

    Returns:
    - dict: A dictionary mapping metadata to its value provided from the API
    query of specified parameters.
    """
    LOGGER.info("Provides the metadata for query of specified parameters.")

    search_data = None
    try:
        request_url = get_request_url(time=time)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        # Send GET request to YouTube API
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
    """Writes the header row to file to contain YouTube data."""
    LOGGER.info("Writing the header row to file to contain YouTube data.")

    with open(DATA_WRITE_FILE, "w") as f:
        f.write("LICENSE TYPE,Document Count\n")
    with open(DATA_WRITE_FILE_TIME, "w") as f:
        f.write("LICENSE TYPE,Time,Document Count\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    LOGGER.info(
        "Recording the data of all license types "
        "findable in the license list "
        "and records into DATA_WRITE_FILE"
    )

    with open(DATA_WRITE_FILE, "a") as f:
        f.write(
            "licenses/by/3.0,"
            f"{get_response_elems()['pageInfo']['totalResults']}\n"
        )


def record_all_licenses_time():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    LOGGER.info(
        "Recording the data of all license types "
        "findable in the license list and records "
        "into DATA_WRITE_FILE, incorporating time"
    )

    with open(DATA_WRITE_FILE_TIME, "a") as f:
        for time in get_next_time_search_interval():
            f.write(
                "licenses/by/3.0,"
                f"{time[2]}-{time[3]},"
                f"{get_response_elems(time=time)['pageInfo']['totalResults']}"
                "\n"
            )


def main():
    set_up_data_file()
    record_all_licenses()
    record_all_licenses_time()


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

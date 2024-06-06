#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for Github Data.
"""

# Standard library
import os
import sys
import traceback

# Third-party
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup paths, Date and LOGGER using quantify.setup()
PATH_REPO_ROOT, PATH_WORK_DIR, _, DATETIME_TODAY, LOGGER = quantify.setup(
    __file__
)

# Set up file path for CSV report
DATA_WRITE_FILE = os.path.join(
    PATH_WORK_DIR,
    "data_github_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def set_up_data_file():
    """Writes the header row of the data file."""
    header_title = "LICENSE_TYPE,Repository Count"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def get_response_elems(license):
    """Provides the metadata for query of specified parameters
    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    LOGGER.info("Providing the metadata for query of specified License")
    try:
        base_url = "https://api.github.com/search/repositories?q=license:"
        request_url = f"{base_url}{license}"
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
        raise
    except requests.RequestException as e:
        LOGGER.error(f"Request Exception: {e}")
        raise
    except KeyError as e:
        LOGGER.error(f"KeyError: {e}. Search data is: {search_data}")
        raise


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to file to contain Github Query data.
    Args:
        license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    """
    data_log = (
        f"{license_type},"
        f"{get_response_elems(license_type)['totalResults']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    licenses = ["CC0-1.0", "CC-BY-4.0", "CC-BY-SA-4.0"]
    for license in licenses:
        record_license_data(license)


def main():
    set_up_data_file()
    record_all_licenses()


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

#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for WikiCommons
Data.
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

# Setup PATH_WORK_DIR, Date and LOGGER using quantify.setup()
_, PATH_WORK_DIR, _, DATETIME_TODAY, LOGGER = quantify.setup(__file__)
# Set up file path for CSV report
DATA_WRITE_FILE = os.path.join(
    PATH_WORK_DIR,
    "data_wikicommons_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_content_request_url(license):
    """Provides the API Endpoint URL for specified parameters' WikiCommons
    contents.

    Args:
    -   license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
    -   string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    LOGGER.info(
        "Providing the API Endpoint URL for specified parameters' WikiCommons."
    )

    return (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&prop=categoryinfo&titles="
        f"Category:{license}&format=json"
    )


def get_subcat_request_url(license):
    """Provides the API Endpoint URL for specified parameters' WikiCommons
    subcategories for recursive searching.

    Args:
    -   license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
    -   string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    LOGGER.info(
        "Providing the API Endpoint URL "
        "for specified parameters' WikiCommons "
        "subcategories for recursive searching."
    )

    base_url = (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&cmtitle="
        f"Category:{license}"
        r"&cmtype=subcat&list=categorymembers&format=json"
    )
    return base_url


def get_subcategories(license, session):
    """Obtain the subcategories of LICENSE in WikiCommons Database for
    recursive searching.

    Args:
    -   license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    -   session:
            A requests.Session object for accessing API endpoints and
            retrieving API endpoint responses.

    Returns:
    -   list: A list representing the subcategories of current license type
        in WikiCommons dataset from a provided API Endpoint URL for the query
        specified by this function's parameters.
    """
    LOGGER.info(
        "Obtaining the subcategories of "
        "LICENSE in WikiCommons Database "
        "for recursive searching."
    )

    try:
        request_url = get_subcat_request_url(license)
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        category_list = []
        for members in search_data["query"]["categorymembers"]:
            category_list.append(
                members["title"].replace("Category:", "").replace("&", "%26")
            )
        return category_list
    except Exception as e:
        if "queries" not in search_data:
            LOGGER.error(
                (
                    f"search data is: \n{search_data} for license {license}"
                    f"This query will not be processed due to empty result."
                )
            )

            sys.exit(1)
        else:
            LOGGER.error(f"Error occurred during request: {e}")
            raise e


def get_license_contents(license, session):
    """Provides the metadata for query of specified parameters.

    Args:
    -   license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    -   session:
            A requests.Session object for accessing API endpoints and
            retrieving API endpoint responses.

    Returns:
    -   dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    LOGGER.info("Providing the metadata for query of specified parameters.")

    try:
        request_url = get_content_request_url(license)
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        file_cnt = 0
        page_cnt = 0
        for id in search_data["query"]["pages"]:
            lic_content = search_data["query"]["pages"][id]
            file_cnt += lic_content["categoryinfo"]["files"]
            page_cnt += lic_content["categoryinfo"]["pages"]
        search_data_dict = {
            "total_file_cnt": file_cnt,
            "total_page_cnt": page_cnt,
        }
        return search_data_dict
    except Exception as e:
        if "queries" not in search_data:
            LOGGER.error(
                (
                    f"search data is: \n{search_data} for license {license}"
                    f"This query will not be processed due to empty result."
                )
            )

            sys.exit(1)
        else:
            LOGGER.error(f"Error occurred during request: {e}")
            raise e


def set_up_data_file():
    """Writes the header row to file to contain WikiCommons Query data."""
    LOGGER.info(
        "Writing the header row to file to contain WikiCommons Query data."
    )

    header_title = "LICENSE TYPE,File Count,Page Count\n"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(header_title)


def record_license_data(license_type, license_alias, session):
    """Writes the row for LICENSE_TYPE to file to contain WikiCommon Query.

    Args:
    -   license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    -   license_alias:
            A forward slash separated string that stands for the route by which
            this license is found from other parent categories. Used for
            eventual efforts of aggregating data.
    -   session:
            A requests.Session object for accessing API endpoints and
            retrieving API endpoint responses.
    """
    LOGGER.info(
        "Writing the row for LICENSE_TYPE to file to contain WikiCommon Query."
    )

    search_result = get_license_contents(license_type, session)
    cleaned_alias = license_alias.replace(",", "|")
    data_log = (
        f"{cleaned_alias},"
        f"{search_result['total_file_cnt']},{search_result['total_page_cnt']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def recur_record_all_licenses(license_alias="Free_Creative_Commons_licenses"):
    """Recursively records the data of all license types findable in the
    license list and its individual subcategories, then records these data into
    the DATA_WRITE_FILE as specified in that constant.

    Due to possible cycles in paths between arbitrary subcategories, a local
    variable LICENSE_CACHE is introduced as a measure of cycle detection to
    prevent re-recording detected subcategories in prior runs.

    Args:
    -   license_alias:
            A forward slash separated string that stands for the route by which
            this license is found from other parent categories. Used for
            eventual efforts of aggregating data. Defaults to
            "Free_Creative_Commons_licenses".
    """
    LOGGER.info(
        "Recursively recording the data of all "
        "license types findable in the license "
        "lists and recording into DATA_WRITE_FILE"
    )

    license_cache = {}
    session = requests.Session()
    max_retries = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=[403, 408, 429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=max_retries))

    def recursive_traversing_subroutine(alias):
        alias.replace(",", "|")
        cur_category = alias.split("/")[-1]
        subcategories = get_subcategories(cur_category, session)
        if cur_category not in license_cache:
            record_license_data(cur_category, alias, session)
            license_cache[cur_category] = True
            for cats in subcategories:
                recursive_traversing_subroutine(f"{alias}/{cats}")

    recursive_traversing_subroutine(license_alias)


def main():
    set_up_data_file()
    recur_record_all_licenses()


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

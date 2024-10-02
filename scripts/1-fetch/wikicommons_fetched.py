#!/usr/bin/env python
"""
This file is dedicated to querying data from WikiCommons.
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
        description="WikiCommons Data Fetching Script"
    )
    parser.add_argument(
        "--license_alias",
        type=str,
        default="Free_Creative_Commons_licenses",
        help="Root category for recursive license search",
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE TYPE,File Count,Page Count\n"
    with open(
        os.path.join(PATHS["data_phase"], "wikicommons_fetched.csv"), "w"
    ) as f:
        f.write(header)


def get_content_request_url(license):
    """
    Provides the API Endpoint URL for
    specified parameters' WikiCommons contents.

    Args:
        license: A string representing the type of license.

    Returns:
        string: The API Endpoint URL for the
        query specified by this function's parameters.
    """
    LOGGER.info(f"Generating content request URL for license: {license}")
    return (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&prop=categoryinfo&titles="
        f"Category:{license}&format=json"
    )


def get_subcat_request_url(license):
    """
    Provides the API Endpoint URL for specified parameters'
    WikiCommons subcategories for recursive searching.

    Args:
        license: A string representing the type of license.

    Returns:
        string: The API Endpoint URL for the query
        specified by this function's parameters.
    """
    LOGGER.info(f"Generating subcategory request URL for license: {license}")
    base_url = (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&cmtitle="
        f"Category:{license}"
        r"&cmtype=subcat&list=categorymembers&format=json"
    )
    return base_url


def get_subcategories(license, session):
    """
    Obtain the subcategories of LICENSE in
    WikiCommons Database for recursive searching.

    Args:
        license: A string representing the type of license.
        session: A requests.Session object for accessing API endpoints.

    Returns:
        list: A list representing the subcategories
        of current license type in WikiCommons dataset.
    """
    LOGGER.info(f"Obtaining subcategories for license: {license}")
    try:
        request_url = get_subcat_request_url(license)
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        category_list = [
            members["title"].replace("Category:", "").replace("&", "%26")
            for members in search_data["query"]["categorymembers"]
        ]
        return category_list
    except Exception as e:
        LOGGER.error(f"Error occurred during subcategory request: {e}")
        raise shared.QuantifyingException(
            f"Error fetching subcategories: {e}", 1
        )


def get_license_contents(license, session):
    """
    Provides the metadata for a query of specified parameters.

    Args:
        license: A string representing the type of license.
        session: A requests.Session object for accessing API endpoints.

    Returns:
        dict: A dictionary mapping metadata
        to its value provided from the API query.
    """
    LOGGER.info(f"Querying content for license: {license}")
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
        return {"total_file_cnt": file_cnt, "total_page_cnt": page_cnt}
    except Exception as e:
        LOGGER.error(f"Error occurred during content request: {e}")
        raise shared.QuantifyingException(f"Error fetching content: {e}", 1)


def record_results(license_type, data):
    """
    Records the data for a specific license type into the CSV file.

    Args:
        license_type: The license type.
        data: A dictionary containing the data to record.
    """
    LOGGER.info(f"Recording data for license: {license_type}")
    row = [license_type, data["total_file_cnt"], data["total_page_cnt"]]
    with open(
        os.path.join(PATHS["data_phase"], "wikicommons_fetched.csv"),
        "a",
        newline="",
    ) as f:
        writer = csv.writer(f, dialect="unix")
        writer.writerow(row)


def recur_record_all_licenses(license_alias="Free_Creative_Commons_licenses"):
    """
    Recursively records the data of all license
    types findable in the license list and its individual subcategories.

    Args:
        license_alias: The root category alias for recursive search.
    """
    LOGGER.info("Starting recursive recording of license data.")

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
            license_content = get_license_contents(cur_category, session)
            record_results(alias, license_content)
            license_cache[cur_category] = True
            for cats in subcategories:
                recursive_traversing_subroutine(f"{alias}/{cats}")

    recursive_traversing_subroutine(license_alias)


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved (wikicommons)": 0}


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
    total_docs_retrieved = state["total_records_retrieved (wikicommons)"]
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
    recur_record_all_licenses(args.license_alias)

    # Update the state with the new count of retrieved records
    total_docs_retrieved += 1  # Update with actual number retrieved
    LOGGER.info(
        f"Total documents retrieved after fetching: {total_docs_retrieved}"
    )
    state["total_records_retrieved (wikicommons)"] = total_docs_retrieved
    save_state(state)

    # Add and commit changes
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_quarter"], "Add and commit WikiCommons data"
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

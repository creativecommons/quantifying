#!/usr/bin/env python
"""
This file is dedicated to querying data from the Google Custom Search API.
"""
# Standard library
import argparse
import csv
import os
import re
import sys
import time
import traceback

# import time
import urllib.parse

# Third-party
import googleapiclient.discovery
import yaml
from dotenv import load_dotenv
from googleapiclient.errors import HttpError

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Constants
DEVELOPER_KEY = os.getenv("GCS_DEVELOPER_KEY")
CX = os.getenv("GCS_CX")
BASE_URL = "https://www.googleapis.com/customsearch/v1"

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_search_service():
    """
    Creates and returns the Google Custom Search API service.
    """
    LOGGER.info("Getting Google Custom Search API Service.")
    return googleapiclient.discovery.build(
        "customsearch", "v1", developerKey=DEVELOPER_KEY, cache_discovery=False
    )


def fetch_results(
    args, service, start_index: int, cr=None, lr=None, link_site=None
) -> int:
    """
    Fetch search results from Google Custom Search API.
    Returns the total number of search results.
    """
    LOGGER.info(
        "Fetching and returning number of search results "
        "from Google Custom Search API"
    )
    records_per_query = args.records
    max_retries = 5
    initial_delay = 1  # in seconds

    LOGGER.info(f"Records per query: {records_per_query}")

    for attempt in range(max_retries):
        try:
            # Added initial query_params parameter for logging purposes
            query_params = {
                "cx": CX,
                "num": records_per_query,
                "start": start_index,
                "cr": cr,
                "lr": lr,
                "q": link_site,
            }
            # Filter out None values
            query_params = {
                k: v for k, v in query_params.items() if v is not None
            }

            LOGGER.info(f"Query Parameters: {query_params}")

            results = service.cse().list(**query_params).execute()

            total_results = int(
                results.get("searchInformation", {}).get("totalResults", 0)
            )
            LOGGER.info(f"Total Results: {total_results}")
            return total_results

        except HttpError as e:
            if e.status_code == 429:
                LOGGER.warning(
                    f"{e.status_code}: {e.reason}. retrying in {initial_delay}"
                    " seconds"
                )
                time.sleep(initial_delay)
                initial_delay *= 2  # Exponential backoff
            else:
                LOGGER.error(f"Error fetching results: {e}")
                return 0
    LOGGER.error("Max tries exceeded. Could not complete the request.")
    return 0


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description="Google Custom Search Script")
    parser.add_argument(
        "--records", type=int, default=1, help="Number of records per query"
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to query"
    )
    parser.add_argument(
        "--licenses", type=int, default=1, help="Number of licenses to query"
    )
    return parser.parse_args()


def set_up_data_file():
    """
    Sets up the data files for recording results.
    Results are currently grouped by location (country) and language
    """
    LOGGER.info("Setting up the data files for recording results.")
    header = (
        "LICENSE TYPE, No Priori, United States, Canada, "
        "India, United Kingdom, Australia, Japan, "
        "English, Spanish, French, Arabic, "
        "Chinese (Simplified), Indonesian\n"
        # "LICENSE TYPE,No Priori,Australia,Brazil,Canada,Egypt,"
        # "Germany,India,Japan,Spain,"
        # "United Kingdom,United States,Arabic,"
        # "Chinese (Simplified),Chinese (Traditional),"
        # "English,French,Indonesian,Portuguese,Spanish\n"
    )
    # open 'w' = open a file for writing
    with open(os.path.join(PATHS["data_phase"], "gcs_fetched.csv"), "w") as f:
        f.write(header)


# State Management
def load_state():
    """
    Loads the state from a JSON file, returns the last fetched start index.
    """
    if os.path.exists(PATHS["state"]):
        with open(PATHS["state"], "r") as f:
            return yaml.safe_load(f)
    return {"total_records_retrieved": 0}


def save_state(state: dict):
    """
    Saves the state to a JSON file.
    Parameters:
        state_file: Path to the state file.
        start_index: Last fetched start index.
    """
    with open(PATHS["state"], "w") as f:
        yaml.safe_dump(state, f)


def get_license_list(args):
    """
    Provides the list of licenses from Creative Commons.

    Returns:
    - np.array:
            An np array containing all license types that should be searched
            via Programmable Search Engine (PSE).
    """
    LOGGER.info("Providing the list of licenses from Creative Commons")
    license_list = []
    with open(
        os.path.join(PATHS["data"], "legal-tool-paths.txt"), "r"
    ) as file:
        for line in file:
            line = (
                line.strip()
            )  # Strip newline and whitespace characters from the line
            match = re.search(r"((?:[^/]+/){2}(?:[^/]+)).*", line)
            if match:
                license_list.append(
                    f"https://creativecommons.org/{match.group(1)}"
                )
    return list(set(license_list))[
        : args.licenses
    ]  # Only the first license for testing
    # Change [:1] to [args.licenses] later, to limit based on args


def get_country_list(select_all=False):
    """
    Provides the list of countries to find Creative Commons usage data on.
    LISTED BY API COUNTRY CODE
    """
    LOGGER.info("Providing the list of countries to find CC usage data on.")
    # countries = []
    # with open(
    #     os.path.join(PATHS["data"], "google_countries.tsv"), "r"
    # ) as file:
    #     for line in file:
    #         country = line.strip().split("\t")[0]
    #         country = country.replace(",", " ")
    #         countries.append(country)

    # if select_all:
    #     return sorted(countries)

    # selected_countries = [
    #     "India",
    #     "Japan",
    #     "United States",
    #     "Canada",
    #     "Brazil",
    #     "Germany",
    #     "United Kingdom",
    #     "Spain",
    #     "Australia",
    #     "Egypt",
    # ]
    # return sorted(
    #     [country for country in countries if country in selected_countries]
    # )

    # Commented out for testing purposes
    return ["US", "CA", "IN", "UK", "AU", "JP"]


def get_lang_list():
    """
    Provides the list of languages to find Creative Commons usage data on.
    LISTED BY API LANGUAGE ABBREVIATION
    """
    LOGGER.info("Providing the list of languages to find CC usage data on.")
    # languages = []
    # with open(
    #     os.path.join(PATHS["data"], "google_lang.txt"), "r"
    # ) as file:
    #     for line in file:
    #         match = re.search(r'"([^"]+)"', line)
    #         if match:
    #             languages.append(match.group(1))

    # selected_languages = [
    #     "Arabic",
    #     "Chinese (Simplified)",
    #     "Chinese (Traditional)",
    #     "English",
    #     "French",
    #     "Indonesian",
    #     "Portuguese",
    #     "Spanish",
    # ]
    # return sorted([lang for lang in languages if lang in selected_languages])

    # Commented out for testing purposes
    return ["en", "es", "fr", "ar", "zh-CH", "id"]


def retrieve_license_data(args, service, license_list):
    """
    Retrieves the data of all license types.
    """
    LOGGER.info("Retrieving the data of all license types.")
    selected_countries = get_country_list()
    selected_languages = get_lang_list()

    data = []

    for license_type in license_list:
        encoded_license = urllib.parse.quote(license_type, safe=":/")
        row = [license_type]
        no_priori_search = fetch_results(
            args, service, start_index=1, link_site=encoded_license
        )
        row.append(no_priori_search)

        for country in selected_countries:
            country_data = fetch_results(
                args,
                service,
                start_index=1,
                cr=f"country{country}",
                link_site=encoded_license,
            )
            row.append(country_data)

        for language in selected_languages:
            language_data = fetch_results(
                args,
                service,
                start_index=1,
                lr=f"lang_{language}",
                link_site=encoded_license,
            )
            row.append(language_data)

        data.append(row)

    # Print the collected data for debugging
    # Data Row Format: [License, No_Priori, United States, English]
    for row in data:
        LOGGER.info(f"Collected data row: {row}")

    return data


def record_results(results):
    """
    Records the search results into the CSV file.
    """
    LOGGER.info("Recording the search results into the CSV file.")
    # open 'a' = Open for appending at the end of the file without truncating
    with open(
        os.path.join(PATHS["data_phase"], "gcs_fetched.csv"), "a", newline=""
    ) as f:
        writer = csv.writer(f)
        for result in results:
            writer.writerow(result)


def main():
    # Added comment for push purposes

    args = parse_arguments()
    state = load_state()
    total_records_retrieved = state["total_records_retrieved"]
    LOGGER.info(f"Initial total_records_retrieved: {total_records_retrieved}")
    goal_records = 1000  # Set goal number of records

    if total_records_retrieved >= goal_records:
        LOGGER.info(
            f"Goal of {goal_records} records already achieved."
            "No further action required."
        )
        return

    shared.log_paths(LOGGER, PATHS)

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if total_records_retrieved == 0:
        set_up_data_file()

    service = get_search_service()
    license_list = get_license_list(args)

    data = retrieve_license_data(args, service, license_list)
    LOGGER.info(f"Final Data: {data}")
    record_results(data)

    # Save the state checkpoint after fetching
    total_records_retrieved += sum(
        len(row) - 1 for row in data
    )  # Exclude license type row
    LOGGER.info(
        f"total_records_retrieved after fetching: {total_records_retrieved}"
    )
    state["total_records_retrieved"] = total_records_retrieved
    save_state(state)


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

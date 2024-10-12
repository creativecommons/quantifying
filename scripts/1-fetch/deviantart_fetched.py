#!/usr/bin/env python
"""
This file is dedicated to querying data from the DeviantArt API.
"""

# Standard library
import argparse
import csv
import os
import sys
import traceback

# Third-party
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Global Variable for API_KEYS indexing
API_KEYS_IND = 0

# Gets API_KEYS and PSE_KEY from .env file
API_KEYS = os.getenv("GOOGLE_API_KEYS").split(",")
PSE_KEY = os.getenv("PSE_KEY")

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(
        description="DeviantArt Data Fetching Script"
    )
    parser.add_argument(
        "--licenses", type=int, default=10, help="Number of licenses to query"
    )
    try:
        args = parser.parse_args()
        LOGGER.info("Arguments parsed successfully: %s", args)
        return args
    except SystemExit as e:
        LOGGER.error("Argument parsing failed with error code %s. This usually indicates invalid command-line input.", e.code)
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(e.code)
    except Exception as e:
        LOGGER.error("An unexpected error occurred during argument parsing. Error: %s", str(e))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)


def set_up_data_file():
    """
    Sets up the data file for recording results.
    """
    LOGGER.info("Setting up the data file for recording results.")
    header = "LICENSE TYPE,Document Count\n"
    try:
        with open(
            os.path.join(PATHS["data_phase"], "deviantart_fetched.csv"), "w"
        ) as f:
            f.write(header)
        LOGGER.info("Data file 'deviantart_fetched.csv' has been created successfully.")
    except FileNotFoundError as fnf_error:
        LOGGER.error("FileNotFoundError: The directory specified in PATHS['data_phase'] does not exist. Error: %s", str(fnf_error))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)
    except IOError as io_error:
        LOGGER.error("IOError: An error occurred while trying to write to the data file. Error: %s", str(io_error))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        LOGGER.error("An unexpected error occurred while setting up the data file. Error: %s", str(e))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)


def get_license_list():
    """
    Provides the list of licenses from Creative Commons.

    Returns:
        list: A list containing all license types that should be searched.
    """
    LOGGER.info("Retrieving list of licenses from Creative Commons' record.")
    try:
        cc_license_data = pd.read_csv(
            os.path.join(PATHS["repo"], "legal-tool-paths.txt"), header=None
        )
        LOGGER.info("Successfully retrieved license data from 'legal-tool-paths.txt'.")
    except FileNotFoundError as fnf_error:
        LOGGER.error("FileNotFoundError: The file 'legal-tool-paths.txt' was not found in the specified repository path. Error: %s", str(fnf_error))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)
    except pd.errors.EmptyDataError as empty_error:
        LOGGER.error("EmptyDataError: The file 'legal-tool-paths.txt' is empty or cannot be read. Error: %s", str(empty_error))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)
    except pd.errors.ParserError as parser_error:
        LOGGER.error("ParserError: There was an error parsing the file 'legal-tool-paths.txt'. Please check the file format. Error: %s", str(parser_error))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        LOGGER.error("An unexpected error occurred while retrieving the license list. Error: %s", str(e))
        LOGGER.debug("Detailed traceback: %s", traceback.format_exc())
        sys.exit(1)

    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    
    LOGGER.info("License list retrieved successfully. Total licenses found: %d", len(license_list))
    return license_list


def get_request_url(license_type):
    """
    Provides the API Endpoint URL for a specified license type.

    Args:
        license_type: A string representing the type of license.

    Returns:
        str: The API Endpoint URL for the query specified by parameters.
    """
    LOGGER.info(f"Generating API Endpoint URL for license: {license_type}")
    try:
        api_key = API_KEYS[API_KEYS_IND]
        return (
            "https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}"
            "&q=_&relatedSite=deviantart.com"
            f'&linkSite=creativecommons.org{license_type.replace("/", "%2F")}'
        )
    except IndexError:
        LOGGER.error("Attempted to access API key at index %d, but it is out of bounds. Depleted all API Keys provided", API_KEYS_IND)
        raise shared.QuantifyingException("No API keys left to use. Please check your API keys configuration.", 1)



def get_response_elems(license_type):
    """
    Retrieves the number of documents for the specified license type.

    Args:
        license_type: A string representing the type of license.

    Returns:
        dict: A dictionary containing the total document count.
    """
    LOGGER.info(f"Querying metadata for license: {license_type}")
    try:
        request_url = get_request_url(license_type)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 500, 502, 503, 504],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        with session.get(request_url) as response:
            response.raise_for_status()  # Raise an error for bad responses
            search_data = response.json()
        return {
            "totalResults": search_data["searchInformation"]["totalResults"]
        }
    except requests.exceptions.HTTPError as e:
        global API_KEYS_IND
        API_KEYS_IND += 1
        LOGGER.error(
            f"HTTP Error encountered while querying license '{license_type}': {e}. "
            "Switching to the next API key."
        )
        if API_KEYS_IND < len(API_KEYS):
            return get_response_elems(license_type)
        else:
            raise shared.QuantifyingException(
                f"HTTP Error encountered for license '{license_type}': {e}. No API keys left.",
                1
            )
    except requests.RequestException as e:
        LOGGER.error(
            f"General Request Exception while querying license '{license_type}': {e}. "
            "This may indicate network issues or invalid requests."
        )
        raise shared.QuantifyingException(f"Request Exception for license '{license_type}': {e}", 1)
    except KeyError as e:
        LOGGER.error(
            f"KeyError: {e}. The expected key was not found in the response for license '{license_type}'. "
            "Please check if the API response structure has changed."
        )
        raise shared.QuantifyingException(f"KeyError for license '{license_type}': {e}", 1)



def retrieve_license_data(args):
    """
    Retrieves the data of all license types specified.

    Args:
        args: Parsed command-line arguments.

    Returns:
        int: The total number of documents retrieved.
    """
    LOGGER.info("Retrieving the data for all license types.")
    licenses = get_license_list()[: args.licenses]

    # data = []
    total_docs_retrieved = 0

    for license_type in licenses:
        try:
            data_dict = get_response_elems(license_type)
            total_docs_retrieved += int(data_dict["totalResults"])
            record_results(license_type, data_dict)
            LOGGER.info(
                f"Successfully retrieved and recorded data for license type '{license_type}': "
                f"{data_dict['totalResults']} documents found."
            )
        except shared.QuantifyingException as e:
            LOGGER.error(
                f"Error while retrieving data for license type '{license_type}': {e}. "
                "Moving on to the next license type."
            )
        except Exception as e:
            LOGGER.error(
                f"An unexpected error occurred while processing license type '{license_type}': {e}. "
                "Please check the logs for more details."
            )

    LOGGER.info(f"Total documents retrieved across all license types: {total_docs_retrieved}")
    return total_docs_retrieved



def record_results(license_type, data):
    """
    Records the data for a specific license type into the CSV file.

    Args:
        license_type: The license type.
        data: A dictionary containing the data to record.
    """
    LOGGER.info(f"Recording data for license: {license_type}")
    row = [license_type, data["totalResults"]]

    try:
        with open(
            os.path.join(PATHS["data_phase"], "deviantart_fetched.csv"),
            "a",
            newline="",
        ) as f:
            writer = csv.writer(f, dialect="unix")
            writer.writerow(row)
            LOGGER.info(
                f"Successfully recorded data for license '{license_type}': "
                f"{data['totalResults']} documents."
            )
    except IOError as e:
        LOGGER.error(
            f"IOError while trying to write data for license '{license_type}': {e}. "
            "Check if the file path is correct and accessible."
        )
    except KeyError as e:
        LOGGER.error(
            f"KeyError: The expected key was not found in the data for license '{license_type}': {e}. "
            "Check the data structure being passed."
        )
    except Exception as e:
        LOGGER.error(
            f"An unexpected error occurred while recording data for license '{license_type}': {e}. "
            "Please check the logs for more details."
        )


def load_state():
    """
    Loads the state from a YAML file, returns the last recorded state.

    Returns:
        dict: The last recorded state.
    """
    LOGGER.info("Attempting to load the state from the YAML file.")

    if os.path.exists(PATHS["state"]):
        try:
            with open(PATHS["state"], "r") as f:
                state = yaml.safe_load(f)
                LOGGER.info("Successfully loaded state from YAML file.")
                return state
        except yaml.YAMLError as e:
            LOGGER.error(f"YAML Error while loading state: {e}. Please check the YAML file for formatting issues.")
        except IOError as e:
            LOGGER.error(f"IOError while trying to read the state file: {e}. Check if the file is accessible.")
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred while loading state: {e}. Please check the logs for more details.")
    
    LOGGER.info("State file does not exist or could not be loaded. Returning default state.")
    return {"total_records_retrieved (deviantart)": 0}


def save_state(state: dict):
    """
    Saves the state to a YAML file.

    Args:
        state: The state dictionary to save.
    """
    LOGGER.info("Attempting to save the state to the YAML file.")
    
    try:
        with open(PATHS["state"], "w") as f:
            yaml.safe_dump(state, f)
            LOGGER.info("Successfully saved the state to the YAML file.")
    except IOError as e:
        LOGGER.error(f"IOError while trying to write to the state file: {e}. Check if the file is writable.")
    except Exception as e:
        LOGGER.error(f"An unexpected error occurred while saving state: {e}. Please check the logs for more details.")


def main():
    """
    Main function to orchestrate the data retrieval and saving process.
    """
    # Fetch and merge changes
    LOGGER.info("Fetching and merging changes from the repository.")
    shared.fetch_and_merge(PATHS["repo"])

    args = parse_arguments()

    state = load_state()
    total_docs_retrieved = state["total_records_retrieved (deviantart)"]
    LOGGER.info(f"Initial total_records_retrieved: {total_docs_retrieved}")

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
    LOGGER.info(f"Data directory created at: {PATHS['data_phase']}")

    if total_docs_retrieved == 0:
        set_up_data_file()

    # Retrieve and record data
    LOGGER.info("Starting the retrieval of license data.")
    docs_retrieved = retrieve_license_data(args)
    LOGGER.info(f"Retrieved {docs_retrieved} documents during this session.")

    # Update the state with the new count of retrieved records
    total_docs_retrieved += docs_retrieved
    LOGGER.info(f"Total documents retrieved after fetching: {total_docs_retrieved}")
    state["total_records_retrieved (deviantart)"] = total_docs_retrieved
    save_state(state)

    # Add and commit changes
    LOGGER.info("Adding and committing changes to the repository.")
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_quarter"], "Add and commit DeviantArt data"
    )

    # Push changes
    LOGGER.info("Pushing changes to the remote repository.")
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
    except Exception as e:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)

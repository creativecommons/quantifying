#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for Wikipedia Data.
"""

# Standard library
import datetime as dt
import os
import sys
import logging

# Third-party
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_wikipedia_{today.year}_{today.month}_{today.day}.csv"
)

# Set up the logger
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Define both the handler and the formatter
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

# Add formatter to the handler
handler.setFormatter(formatter)

# Add handler to the logger
LOG.addHandler(handler)

# Log the start of the script execution
LOG.info("Script execution started.")

def get_wiki_langs():
    """
    Provides the list of language to find Creative Commons usage data on.

    The codes represent the language codes defined by ISO 639-1 and ISO 639-3,
    and the decision of which language code to use is usually determined by the
    IETF language tag policy.
    (https://en.wikipedia.org/wiki/List_of_Wikipedias#Wikipedia_edition_codes)

    Returns:
    - pd.DataFrame: A Dataframe containing information of each Wikipedia
    language and its respective encoding on web address.
    """
    LOG.info("Providing the list of language to find Creative Commons usage data on.")
    return pd.read_csv(f"{CWD}/language-codes_csv.csv")


def get_request_url(lang="en"):
    """
    Provides the API Endpoint URL for specified parameter combinations.

    Args:
    - lang: A string representing the language that the search results are
    presented in. Alternatively, the default value is by Wikipedia customs "en"

    Returns:
    - string: A string representing the API Endpoint URL for the query
    specified by this function's parameters.
    """
    LOG.info("Providing the API Endpoint URL for specified parameter combinations.")
    
    base_url = (
        r"wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=statistics"
        r"&format=json"
    )
    base_url = f"https://{lang}.{base_url}"
    return base_url


def get_response_elems(language="en"):
    """
    Provides the metadata for query of specified parameters

    Args:
    - language: A string representing the language that the search results are
    presented in. Alternatively, the default value is by Wikipedia customs "en"

    Returns:
    - dict: A dictionary mapping metadata to its value provided from the API
    query of specified parameters.
    """
    LOG.info("Providing the metadata for query of specified parameters")
    
    search_data = None
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
            search_data = requests.get(request_url).json()
            search_data_dict = search_data["query"]["statistics"]
            search_data_dict["language"] = language
        return search_data_dict
    except Exception as e:
        if search_data is None:
            LOG.exception(
                f"Received Result is None due to Language {language} absent as"
                "an available Wikipedia client. Will therefore return an empty"
                "dictionary for result, but will continue querying.")
            return {}
        elif "query" not in search_data:
            LOG.exception(f"search data is: \n{search_data}")
            sys.exit(1)
        else:
            raise e


def set_up_data_file():
    """Writes the header row to file to contain Wikipedia Query data."""
    LOG.info("Writing the header row to file to contain Wikipedia Query data.")
    
    header_title = ",".join(get_response_elems())
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_lang_data(lang="en"):
    """Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
    - lang: A string representing the language that the search results are
    presented in. Alternatively, the default value is by Wikipedia customs "en"
    """
    LOG.info("Writing the row for LICENSE_TYPE to file to contain Google Query data.")
    
    response = get_response_elems(lang)
    if response != {}:
        response_values = response.values()
        response_str = [str(elem) for elem in response_values]
        with open(DATA_WRITE_FILE, "a") as f:
            f.write(",".join(response_str) + "\n")


def record_all_licenses():
    """Records the data of all language types findable in the language list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    LOG.info("Recording the data of all language types findable in the language list and recording into DATA_WRITE_FILE")
    
    wiki_langs = get_wiki_langs()
    for iso_language_code in wiki_langs["alpha2"]:
        record_lang_data(iso_language_code)


def get_current_data():
    """Return a DataFrame for the Creative Commons usage data collected, all
    Wikipedia texts are licensed under CC-BY-SA 3.0

    Returns:
    - pd.DataFrame: A DataFrame recording the number of CC-licensed documents
    per search query of assumption.
    """
    LOG.info("Returning a DataFrame for the Creative Commons usage data collected")
    return pd.read_csv(DATA_WRITE_FILE).set_index("language")


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOG.error("System exit with code: %d", e.code)
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOG.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOG.exception("Unhandled exception occurred during script execution:")
        sys.exit(1)

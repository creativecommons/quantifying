#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for Wikipedia Data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

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


def get_wiki_langs():
    """Provides the list of language to find Creative Commons usage data on.

    The codes represent the language codes defined by ISO 639-1 and ISO 639-3,
    and the decision of which language code to use is usually determined by the
    IETF language tag policy.
    (https://en.wikipedia.org/wiki/List_of_Wikipedias#Wikipedia_edition_codes)

    Returns:
        pd.DataFrame: A Dataframe containing information of each Wikipedia
        language and its respective encoding on web address.
    """
    return pd.read_csv(f"{CWD}/language-codes_csv.csv")


def get_request_url(lang="en"):
    """Provides the API Endpoint URL for specified parameter combinations.

    Args:
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default value is by Wikipedia
            customs "en".

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
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
            search_data = response.json()

        if search_data is None:
            print(
                f"Received Result is None due to Language {language} absent as"
                "an available Wikipedia client. Will therefore return an empty"
                "dictionary for result, but will continue querying.",
                file=sys.stderr,
            )
            return {}

        search_data_dict = search_data["query"]["statistics"]
        search_data_dict["language"] = language
        return search_data_dict

    except requests.HTTPError as e:
        print(f"HTTP Error: {e}", file=sys.stderr)
        raise
    except requests.RequestException as e:
        print(f"Request Exception: {e}", file=sys.stderr)
        raise
    except KeyError as e:
        print(f"KeyError: {e}. Search data is: {search_data}", file=sys.stderr)
        raise


def set_up_data_file():
    """Writes the header row to file to contain Wikipedia Query data."""
    header_title = ",".join(get_response_elems())
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_lang_data(lang="en"):
    """Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default value is by Wikipedia
            customs "en".
    """
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
    wiki_langs = get_wiki_langs()
    for iso_language_code in wiki_langs["alpha2"]:
        record_lang_data(iso_language_code)


def get_current_data():
    """Return a DataFrame for the Creative Commons usage data collected, all
    Wikipedia texts are licensed under CC-BY-SA 3.0

    Returns:
        pd.DataFrame: A DataFrame recording the number of CC-licensed documents
        per search query of assumption.
    """
    return pd.read_csv(DATA_WRITE_FILE).set_index("language")


def main():
    set_up_data_file()
    record_all_licenses()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("INFO (130) Halted via KeyboardInterrupt.", file=sys.stderr)
        sys.exit(130)
    except Exception:
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
        sys.exit(1)

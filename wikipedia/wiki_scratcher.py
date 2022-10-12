"""
This file is dedicated to obtain a .csv record report for Google Custom Search 
Data.
"""

# Standard library
import datetime as dt
import os
import random
import sys
import time
import traceback

# Third-party
import pandas as pd
import requests

CWD = os.path.dirname(os.path.abspath(__file__))
CALLBACK_INDEX = 2
CALLBACK_EXPO = 0
MAX_WAIT = 64
DATA_WRITE_FILE = CWD


def expo_backoff():
    """Performs exponential backoff upon call.

    The function will force a wait of CALLBACK_INDEX ** CALLBACK_EXPO + r
    seconds, where r is a decimal number between 0.001 and 0.999, inclusive.
    If that value is higher than MAX_WAIT, then it will just wait MAX_WAIT
    seconds instead.
    """
    global CALLBACK_EXPO
    backoff = random.randint(1, 1000) / 1000 + CALLBACK_INDEX**CALLBACK_EXPO
    time.sleep(min(backoff, MAX_WAIT))
    if backoff < MAX_WAIT:
        CALLBACK_EXPO += 1


def expo_backoff_reset():
    """Resets the CALLBACK_EXPO to 0."""
    global CALLBACK_EXPO
    CALLBACK_EXPO = 0


def get_wiki_langs():
    """Provides the list of language to find Creative Commons usage data on.

    Returns:
        pd.DataFrame: A Dataframe containing information of each Wikipedia
        language and its respective encoding on web address.
    """
    return pd.read_csv(CWD + r"/wiki_langs.txt", sep="\t")


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
    base_url = "https://" + lang + "." + base_url
    return base_url


def get_response_elems(lang="en", eb=False):
    """Provides the metadata for query of specified parameters

    Args:
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default value is by Wikipedia
            customs "en".
        eb:
            A boolean indicating whether there should be exponential callback.
            Is by default False.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    search_data = None
    try:
        url = get_request_url(lang)
        search_data = requests.get(url).json()
        search_data_dict = search_data["query"]["statistics"]
        search_data_dict["language"] = lang
        return search_data_dict
    except:
        if eb:
            expo_backoff()
            get_response_elems()
        elif "pageInfo" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain Wikipedia Query data."""
    header_title = ",".join(get_response_elems())
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(header_title + "\n")


def record_lang_data(lang="en"):
    """Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default value is by Wikipedia
            customs "en".
    """
    response = get_response_elems(lang).values()
    response_str = [str(elem) for elem in response]
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(",".join(response_str) + "\n")


def record_all_licenses():
    """Records the data of all language types findable in the language list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    wiki_langs = get_wiki_langs()
    for l_code in wiki_langs["Wiki"]:
        record_lang_data(l_code)


def get_current_data():
    """Return a DataFrame for the Creative Commons usage data collected, all
    Wikipedia texts are licensed under CC-BY-SA 3.0

    Returns:
        pd.DataFrame: A DataFrame recording the number of CC-licensed documents
        per search query of assumption.
    """
    return pd.read_csv(DATA_WRITE_FILE).set_index("language")


def main():
    # TODO
    global DATA_WRITE_FILE
    today = dt.datetime.today()
    DATA_WRITE_FILE += (
        f"/data_wikipedia_{today.year}_{today.month}_{today.day}.txt"
    )
    set_up_data_file()
    record_all_licenses()
    DATA_WRITE_FILE = CWD


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

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
import query_secret
import requests

CWD = os.path.dirname(os.path.abspath(__file__))
CALLBACK_INDEX = 2
CALLBACK_EXPO = 0
MAX_WAIT = 64
DATA_WRITE_FILE = CWD
API_KEY = query_secret.API_KEY
PSE_KEY = query_secret.PSE_KEY


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


def get_license_list():
    """Provides the list of license from 2018's record of Creative Commons.

    Returns:
        np.array: An np array containing all license types that should be
        searched via Programmable Search Engine.
    """
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    return license_list


def get_lang_list():
    """Provides the list of language to find Creative Commons usage data on.

    Returns:
        pd.DataFrame: A Dataframe whose index is language name and has a column
        for the corresponding language code.
    """
    langs = pd.read_csv(f"{CWD}/google_lang.txt", sep=":")
    langs = langs.set_index("Language")
    selected_langs = langs.iloc[[7, 33, 34, 8, 11, 0, 25, 14], :].sort_index()
    return selected_langs


def get_cntr_list():
    """Provides the list of countries to find Creative Commons usage data on.

    Returns:
        pd.DataFrame: A Dataframe whose index is country name and has a column
        for the corresponding country code.
    """
    cntrs = pd.read_csv(CWD + "/google_cntrs.txt", sep="\t")
    cntrs = cntrs.set_index("Country")
    selected_cntrs = cntrs.loc[
        [
            "India",
            "Japan",
            "United States",
            "Canada",
            "Brazil",
            "Germany",
            "United Kingdom",
            "Spain",
            "Australia",
            "Egypt",
        ],
        :,
    ].sort_index()
    return selected_cntrs


def get_request_url(license=None, cntr=None, lang=None):
    """Provides the API Endpoint URL for specified parameter combinations.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        cntr:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default None value or "all" stands
            for having no assumption about language of document.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    base_url = (
        r"https://customsearch.googleapis.com/customsearch/v1"
        f"?key={API_KEY}&cx={PSE_KEY}"
        r"&q=link%3Acreativecommons.org"
    )
    if license is not None:
        base_url += license.replace("/", "%2F")
    else:
        base_url += "/licenses".replace("/", "%2F")
    if cntr is not None:
        base_url += f"&cr={cntr}"
    if lang is not None:
        base_url += f"&lr={lang}"
    return base_url


def get_response_elems(license=None, cntr=None, lang=None, eb=False):
    """Provides the metadata for query of specified parameters

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        cntr:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default None value or "all" stands
            for having no assumption about language of document.
        eb:
            A boolean indicating whether there should be exponential callback.
            Is by default False.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        url = get_request_url(license=license, cntr=cntr, lang=lang)
        search_data = requests.get(url).json()
        search_data_dict = {
            "totalResults": search_data["queries"]["request"][0][
                "totalResults"
            ]
        }
        return search_data_dict
    except:
        if eb:
            expo_backoff()
            get_response_elems(license, cntr, lang)
        elif "queries" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain Google Query data."""
    header_title = "LICENSE TYPE,No Priori,"
    selected_cntrs = get_cntr_list()
    selected_langs = get_lang_list()
    header_title = (
        "LICENSE TYPE,No Priori,"
        f"{','.join(selected_cntrs.index)}"
        f"{','.join(selected_langs.index)}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(header_title + "\n")


def record_license_data(license_type=None):
    """Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    """
    if license_type is None:
        data_log = "all,"
    else:
        data_log = f"{license_type},"
    selected_cntrs = get_cntr_list()
    selected_langs = get_lang_list()
    no_priori_search = get_response_elems(license=license_type)
    data_log += f"{no_priori_search['totalResults']},"
    for cntr_name in selected_cntrs.iloc[:, 0]:
        response = get_response_elems(license=license_type, cntr=cntr_name)
        data_log += f"{response['totalResults']},"
    for lang_name in selected_langs.iloc[:, 0]:
        response = get_response_elems(license=license_type, lang=lang_name)
        data_log += f"{response['totalResults']},"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_wRITE_FILE as specified in that constant.
    """
    license_list = get_license_list()
    record_license_data()
    for license_type in license_list:
        record_license_data(license_type)


def get_current_data():
    """Return a DataFrame for the Creative Commons usage data collected.

    Returns:
        pd.DataFrame: A DataFrame recording the number of CC-licensed documents
        per search query of assumption.
    """
    return pd.read_csv(DATA_WRITE_FILE).iloc[1:, :-1].set_index("LICENSE TYPE")


def main():
    # TODO
    global DATA_WRITE_FILE
    today = dt.datetime.today()
    DATA_WRITE_FILE += (
        f"/data_google_{today.year}_{today.month}_{today.day}.txt"
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
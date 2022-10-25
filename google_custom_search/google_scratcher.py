"""
This file is dedicated to obtain a .csv record report for Google Custom Search
Data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import pandas as pd
import query_secret
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
API_KEY = query_secret.API_KEY
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}"
    f"/data_google_custom_search_{today.year}_{today.month}_{today.day}.csv"
)
PSE_KEY = query_secret.PSE_KEY


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
    languages = pd.read_csv(
        f"{CWD}/google_lang.txt", sep=": ", header=None, engine="python"
    )
    languages[0] = languages[0].str.extract(r'"(\w+)"')
    languages = languages.set_index(1)
    selected_languages = languages.iloc[
        [7, 33, 34, 8, 11, 0, 25, 14], :
    ].sort_index()
    return selected_languages


def get_country_list():
    """Provides the list of countries to find Creative Commons usage data on.

    Returns:
        pd.DataFrame: A Dataframe whose index is country name and has a column
        for the corresponding country code.
    """
    countries = pd.read_csv(CWD + "/google_countries.txt", sep="\t")
    countries = countries.set_index("Country")
    selected_countries = countries.loc[
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
    return selected_countries


def get_request_url(license=None, country=None, language=None):
    """Provides the API Endpoint URL for specified parameter combinations.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        country:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
        language:
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
    if country is not None:
        base_url += f"&cr={country}"
    if language is not None:
        base_url += f"&lr={language}"
    return base_url


def get_response_elems(license=None, country=None, language=None):
    """Provides the metadata for query of specified parameters

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        country:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
        lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default None value or "all" stands
            for having no assumption about language of document.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        request_url = get_request_url(license, country, language)
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
        search_data_dict = {
            "totalResults": search_data["queries"]["request"][0][
                "totalResults"
            ]
        }
        return search_data_dict
    except Exception:
        if "queries" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain Google Query data."""
    header_title = "LICENSE TYPE,No Priori,"
    selected_countries = get_country_list()
    selected_languages = get_lang_list()
    header_title = (
        "LICENSE TYPE,No Priori,"
        f"{','.join(selected_countries.index)}"
        f"{','.join(selected_languages.index)}"
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
    selected_countries = get_country_list()
    selected_languages = get_lang_list()
    no_priori_search = get_response_elems(license=license_type)
    data_log += f"{no_priori_search['totalResults']},"
    for country_name in selected_countries.iloc[:, 0]:
        response = get_response_elems(
            license=license_type, country=country_name
        )
        data_log += f"{response['totalResults']},"
    for lang_name in selected_languages.iloc[:, 0]:
        response = get_response_elems(license=license_type, language=lang_name)
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

#!/usr/bin/env python
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
import query_secrets
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
API_KEYS = query_secrets.API_KEYS
API_KEYS_IND = 0
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}"
    f"/data_google_custom_search_{today.year}_{today.month}_{today.day}.csv"
)
DATA_WRITE_FILE_TIME = (
    f"{CWD}"
    f"/data_google_custom_search_time_"
    f"{today.year}_{today.month}_{today.day}.csv"
)
SEARCH_HALFYEAR_SPAN = 20
PSE_KEY = query_secrets.PSE_KEY


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
    languages[0] = languages[0].str.extract(r'"([^"]+)"')
    languages = languages.set_index(1)
    selected_languages = languages[
        languages.index.isin(
            [
                "Arabic",
                "Chinese (Simplified)",
                "Chinese (Traditional)",
                "English",
                "French",
                "Indonesian",
                "Portuguese",
                "Spanish",
            ]
        )
    ].sort_index()
    return selected_languages


def get_country_list():
    """Provides the list of countries to find Creative Commons usage data on.

    Returns:
        pd.DataFrame: A Dataframe whose index is country name and has a column
        for the corresponding country code.
    """
    countries = pd.read_csv(CWD + "/google_countries.tsv", sep="\t")
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


def get_request_url(license=None, country=None, language=None, time=False):
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
        time:
            A boolean indicating whether this query is related to video time
            occurrence.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    try:
        api_key = API_KEYS[API_KEYS_IND]
        base_url = (
            r"https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}"
        )
        if time:
            base_url = f"{base_url}&dateRestrict=m{time}"
        base_url = f"{base_url}&q=_&linkSite=creativecommons.org"
        if license is not None:
            base_url = f'{base_url}{license.replace("/", "%2F")}'
        else:
            base_url = f'{base_url}{"/licenses".replace("/", "%2F")}'
        if country is not None:
            base_url = f"{base_url}&cr={country}"
        if language is not None:
            base_url = f"{base_url}&lr={language}"
        return base_url
    except Exception as e:
        if isinstance(e, IndexError):
            print("Depleted all API Keys provided", file=sys.stderr)
        else:
            raise e


def get_response_elems(license=None, country=None, language=None, time=False):
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
        time:
            A boolean indicating whether this query is related to video time
            occurrence.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        request_url = get_request_url(license, country, language, time)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 500, 502, 503, 504],
            # 429 is Quota Limit Exceeded, which will be handled alternatively
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=max_retries))
        with session.get(request_url) as response:
            response.raise_for_status()
            search_data = response.json()
        search_data_dict = {
            "totalResults": search_data["searchInformation"]["totalResults"]
        }
        return search_data_dict
    except Exception as e:
        if isinstance(e, requests.exceptions.HTTPError):
            global API_KEYS_IND
            API_KEYS_IND += 1
            print(
                "Changing API KEYS due to depletion of quota", file=sys.stderr
            )
            return get_response_elems(license, country, language, time)
        else:
            print(f"Request URL was {request_url}", file=sys.stderr)
            raise e


def set_up_data_file():
    """Writes the header row to file to contain Google Query data."""
    header_title = "LICENSE TYPE,No Priori,"
    selected_countries = get_country_list()
    selected_languages = get_lang_list()
    header_title = (
        "LICENSE TYPE,No Priori,"
        f"{','.join(selected_countries.index)},"
        f"{','.join(selected_languages.index)}"
    )
    header_title_time = (
        "LICENSE TYPE,"
        f"{','.join([str(6 * i) for i in range(SEARCH_HALFYEAR_SPAN)])}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{header_title}\n")
    with open(DATA_WRITE_FILE_TIME, "a") as f:
        f.write(f"{header_title_time}\n")


def record_license_data(license_type=None, time=False):
    """Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        time:
            A boolean indicating whether this query is related to video time
            occurrence.
    """
    if license_type is None:
        data_log = "all"
    else:
        data_log = f"{license_type}"
    if not time:
        selected_countries = get_country_list()
        selected_languages = get_lang_list()
        no_priori_search = get_response_elems(license=license_type)
        data_log += f",{no_priori_search['totalResults']}"
        for country_name in selected_countries.iloc[:, 0]:
            response = get_response_elems(
                license=license_type, country=country_name
            )
            data_log += f",{response['totalResults']}"
        for language_name in selected_languages.iloc[:, 0]:
            response = get_response_elems(
                license=license_type, language=language_name
            )
            data_log += f",{response['totalResults']}"
        with open(DATA_WRITE_FILE, "a") as f:
            f.write(f"{data_log}\n")
    else:
        for i in range(SEARCH_HALFYEAR_SPAN):
            time_data = get_response_elems(license=license_type, time=i * 6)
            data_log = f"{data_log},{time_data['totalResults']}"
        with open(DATA_WRITE_FILE_TIME, "a") as f:
            f.write(f"{data_log}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE and DATA_WRITE_FILE_TIME as
    specified in that constant.
    """
    license_list = get_license_list()
    record_license_data(time=False)
    record_license_data(time=True)
    for license_type in license_list:
        record_license_data(license_type, time=False)
        print("DEBUG ", "no time ", license_type)
        record_license_data(license_type, time=True)
        print("DEBUG ", "time ", license_type)


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

#!/usr/bin/env python
"""
This file is dedicated to obtain a .csv record report for
Google Custom Search Data.
"""

# Standard library
import os
import sys
import traceback

# Third-party
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup paths, Date and LOGGER using quantify.setup()
PATH_REPO_ROOT, PATH_WORK_DIR, PATH_DOTENV, DATETIME_TODAY, LOGGER = (
    quantify.setup(__file__)
)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Gets API_KEYS and PSE_KEY from .env file
API_KEYS = os.getenv("GOOGLE_API_KEYS").split(",")
PSE_KEY = os.getenv("PSE_KEY")


# Global Variables for API_KEYS indexing and Search Halfyear Span
API_KEYS_IND = 0
SEARCH_HALFYEAR_SPAN = 20

# Set up file path for CSV report
DATA_WRITE_FILE = os.path.join(
    PATH_WORK_DIR,
    "data_google_custom_search_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)
DATA_WRITE_FILE_TIME = os.path.join(
    PATH_WORK_DIR,
    "data_google_custom_search_time_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)
DATA_WRITE_FILE_COUNTRY = os.path.join(
    PATH_WORK_DIR,
    "data_google_custom_search_country_"
    f"{DATETIME_TODAY.year}_{DATETIME_TODAY.month}_{DATETIME_TODAY.day}.csv",
)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_license_list():
    """
    Provides the list of licenses from 2018's record of Creative Commons.

    Returns:
    - np.array:
            An np array containing all license types that should be searched
            via Programmable Search Engine (PSE).
    """
    # Read license data from file
    cc_license_data = pd.read_csv(
        os.path.join(PATH_REPO_ROOT, "legal-tool-paths.txt"), header=None
    )
    # Define regex pattern to extract license types
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_list = (
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    return license_list


def get_lang_list():
    """
    Provides the list of languages to find Creative Commons usage data on.

    Returns:
    - pd.DataFrame:
                A Dataframe whose index is language name and has a column for
                the corresponding language code.
    """
    LOGGER.info(
        "Providing the list of languages "
        "to find Creative Commons usage data on."
    )

    languages = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "google_lang.txt"),
        sep=": ",
        header=None,
        engine="python",
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


def get_country_list(select_all=False):
    """
    Provides the list of countries to find Creative Commons usage data on.

    Args:
    - select_all:
                A boolean indicating whether the returned list will have all
                countries.

    Returns:
    - pd.DataFrame:
                A Dataframe whose index is country name and has a column for
                the corresponding country code.
    """
    countries = pd.read_csv(PATH_WORK_DIR + "/google_countries.tsv", sep="\t")
    countries["Country"] = countries["Country"].str.replace(",", " ")
    countries = countries.set_index("Country").sort_index()
    if select_all:
        return countries
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
    """
    Provides the API Endpoint URL for specified parameter combinations.

    Args:
    - license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    - country:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
    - language:
            A string representing the language that the search results are
            presented in. Alternatively, the default None value or "all" stands
            for having no assumption about language of document.
    - time:
            A boolean indicating whether this query is related to video time
            occurrence.

    Returns:
    - string:
            A string representing the API Endpoint URL for the query specified
            by this function's parameters.
    """
    LOGGER.info(
        "Providing the API Endpoint URL for specified parameter combinations."
    )

    try:
        api_key = API_KEYS[API_KEYS_IND]
        base_url = (
            r"https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}&q=_"
        )
        if time:
            base_url = f"{base_url}&dateRestrict=m{time}"
        if license != "no":
            base_url = f"{base_url}&linkSite=creativecommons.org"
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
            LOGGER.error("Depleted all API Keys provided")
        else:
            raise e


def get_response_elems(license=None, country=None, language=None, time=False):
    """
    Provides the metadata for query of specified parameters

    Args:
    - license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    - country:
            A string representing the country code of country that the search
            results would be originating from. Alternatively, the default None
            value or "all" stands for having no assumption about country of
            origin.
    - lang:
            A string representing the language that the search results are
            presented in. Alternatively, the default None value or "all" stands
            for having no assumption about language of document.
    - time:
            A boolean indicating whether this query is related to video time
            occurrence.

    Returns:
    - dict:
            A dictionary mapping metadata to its value provided from the API
            query of specified parameters.
    """
    LOGGER.info("Providing the metadata for a query of specified parameters.")

    try:
        # Make a request to the API and handle potential retries
        request_url = get_request_url(license, country, language, time)
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[400, 403, 408, 500, 502, 503, 504],
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
            # If quota limit exceeded, switch to the next API key
            global API_KEYS_IND
            API_KEYS_IND += 1
            LOGGER.error("Changing API KEYS due to depletion of quota")
            return get_response_elems(license, country, language, time)
        else:
            LOGGER.error(f"Request URL was {request_url}")
            raise e


def set_up_data_file():
    # Write header rows in files to contain Google Query data.
    header_title = "LICENSE TYPE,No Priori,"
    selected_countries = get_country_list()
    all_countries = get_country_list(select_all=True)
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
    header_title_country = "LICENSE TYPE," f"{','.join(all_countries.index)}"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")
    with open(DATA_WRITE_FILE_TIME, "w") as f:
        f.write(f"{header_title_time}\n")
    with open(DATA_WRITE_FILE_COUNTRY, "w") as f:
        f.write(f"{header_title_country}\n")


def record_license_data(license_type=None, time=False, country=False):
    """
    Writes the row for LICENSE_TYPE to file to contain Google Query data.

    Args:
    - license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    - time:
            A boolean indicating whether this query is related to video time
            occurrence.
    - country:
            A boolean indicating whether this query is related to country
            occurrence.
    """
    LOGGER.info(
        "Writing the row for LICENSE_TYPE "
        "to file to contain Google Query data."
    )

    if license_type is None:
        data_log = "all"
    else:
        data_log = f"{license_type}"
    if country:
        all_countries = get_country_list(select_all=True)
        for current_country in all_countries.iloc[:, 0]:
            country_license_data = get_response_elems(
                license=license_type, country=current_country
            )
            data_log = f"{data_log},{country_license_data['totalResults']}"
        data_log = f"{data_log}\nAll Documents"
        for current_country in all_countries.iloc[:, 0]:
            country_overall_data = get_response_elems(
                license="no", country=current_country
            )
            data_log = f"{data_log},{country_overall_data['totalResults']}"
        with open(DATA_WRITE_FILE_COUNTRY, "a") as f:
            f.write(f"{data_log}\n")
    elif time:
        for i in range(SEARCH_HALFYEAR_SPAN):
            time_data = get_response_elems(license=license_type, time=i * 6)
            data_log = f"{data_log},{time_data['totalResults']}"
        with open(DATA_WRITE_FILE_TIME, "a") as f:
            f.write(f"{data_log}\n")
    else:
        selected_countries = get_country_list()
        selected_languages = get_lang_list()
        no_priori_search = get_response_elems(license=license_type)
        data_log += f",{no_priori_search['totalResults']}"
        for country_name in selected_countries.iloc[:, 0]:
            response = get_response_elems(
                license=license_type, country=country_name
            )
            data_log = f"{data_log},{response['totalResults']}"
        for language_name in selected_languages.iloc[:, 0]:
            response = get_response_elems(
                license=license_type, language=language_name
            )
            data_log = f"{data_log},{response['totalResults']}"
        with open(DATA_WRITE_FILE, "a") as f:
            f.write(f"{data_log}\n")


def record_all_licenses():
    """
    Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE and DATA_WRITE_FILE_TIME as
    specified in that constant.
    """
    # Record license data with no assumption about license type
    record_license_data()
    record_license_data(time=True)
    record_license_data(country=True)
    # Gets the list of license types and record data for each license type
    license_list = get_license_list()
    for license_type in license_list:
        record_license_data(license_type)
        record_license_data(license_type, time=True)


def main():
    set_up_data_file()
    record_all_licenses()


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

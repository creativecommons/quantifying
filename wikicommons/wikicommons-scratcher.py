"""
This file is dedicated to obtain a .csv record report for WikiCommons
Data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

today = dt.datetime.today()
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}" f"/data_wikicommons_{today.year}_{today.month}_{today.day}.csv"
)
LICENSE_CACHE = {}


def get_content_request_url(license):
    """Provides the API Endpoint URL for specified parameters' WikiCommons
    contents.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    return (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&prop=categoryinfo&titles="
        f"Category:{license}&format=json"
    )


def get_subcat_request_url(license):
    """Provides the API Endpoint URL for specified parameters' WikiCommons
    subcategories for recursive searching.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    base_url = (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&cmtitle="
        f"Category:{license}"
        r"&cmtype=subcat&list=categorymembers&format=json"
    )
    return base_url


def get_subcategories(license):
    """Obtain the subcategories of LICENSE in WikiCommons Database for
    recursive searching.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
        list: A list representing the subcategories of current license type
        in WikiCommons dataset from a provided API Endpoint URL for the query
        specified by this function's parameters.
    """
    try:
        request_url = get_subcat_request_url(license)
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
        category_list = []
        for members in search_data["query"]["categorymembers"]:
            category_list.append(
                members["title"].replace("Category:", "").replace("&", "%26")
            )
        return category_list
    except Exception:
        if "query" not in search_data:
            print(search_data)
            print("This query will not be processed due to empty subcats.")
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def get_license_contents(license):
    """Provides the metadata for query of specified parameters.

    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        request_url = get_content_request_url(license)
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
        file_cnt = 0
        page_cnt = 0
        for id in search_data["query"]["pages"]:
            lic_content = search_data["query"]["pages"][id]
            file_cnt += lic_content["categoryinfo"]["files"]
            page_cnt += lic_content["categoryinfo"]["pages"]
        search_data_dict = {
            "total_file_cnt": file_cnt,
            "total_page_cnt": page_cnt,
        }
        return search_data_dict
    except Exception:
        if "queries" not in search_data:
            print(search_data)
            print("This query will not be processed due to empty result.")
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain WikiCommons Query data."""
    header_title = "LICENSE TYPE,File Count,Page Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(header_title + "\n")


def record_license_data(license_type, license_alias):
    """Writes the row for LICENSE_TYPE to file to contain WikiCommon Query.

    Args:
        license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        license_alias:
            A forward slash separated string that stands for the route by which
            this license is found from other parent categories. Used for
            eventual efforts of aggregating data.
    """
    search_result = get_license_contents(license_type)
    cleaned_alias = license_alias.replace(",", "|")
    data_log = (
        f"{cleaned_alias},"
        f"{search_result['total_file_cnt']},{search_result['total_page_cnt']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def recur_record_all_licenses(alias="Free_Creative_Commons_licenses"):
    """Recursively records the data of all license types findable in the
    license list and its individual subcategories, then records these data into
    the DATA_wRITE_FILE as specified in that constant.

    Args:
        license_alias:
            A forward slash separated string that stands for the route by which
            this license is found from other parent categories. Used for
            eventual efforts of aggregating data. Defaults to
            "Free_Creative_Commons_licenses".
    """
    alias.replace(",", "|")
    cur_category = alias.split("/")[-1]
    subcategories = get_subcategories(cur_category)
    if cur_category not in LICENSE_CACHE:
        record_license_data(cur_category, alias)
        LICENSE_CACHE[cur_category] = True
        print("DEBUG", f"Logged {cur_category} from {alias}")
        for cats in subcategories:
            recur_record_all_licenses(alias=f"{alias}/{cats}")


def main():
    set_up_data_file()
    recur_record_all_licenses()


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

"""
This file is dedicated to obtain a .csv record report for internetarchive
data.
"""

# Standard library
import datetime as dt
import os
import sys
import traceback

# Third-party
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# First-party/Local
from internetarchive.search import Search
from internetarchive.session import ArchiveSession

today = dt.datetime.today()
CWD = os.path.dirname(os.path.abspath(__file__))
DATA_WRITE_FILE = (
    f"{CWD}"
    f"/data_internetarchive_{today.year}_{today.month}_{today.day}.csv"
)


def get_license_list():
    """Provides the list of license from a Creative Commons provided tool list.
    Returns:
        np.array: An np array containing all license types that should be
        searched via Programmable Search Engine.
    """
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    license_list = cc_license_data[0].unique()
    return license_list


def get_response_elems(license):
    """Provides the metadata for query of specified parameters
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
        max_retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        )
        search_session = ArchiveSession()
        search_session.mount_http_adapter(
            protocol="https://",
            max_retries=HTTPAdapter(max_retries=max_retries),
        )
        search_data = Search(
            search_session,
            f'/metadata/licenseurl:("http://creativecommons.org/{license}")',
        )
        search_data_dict = {"totalResults": len(search_data)}
        return search_data_dict
    except Exception as e:
        raise e


def set_up_data_file():
    """Writes the header row to file to contain IA data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "w") as f:
        f.write(f"{header_title}\n")


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to file to contain IA Query data.
    Args:
        license_type:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
    """
    data_log = (
        f"{license_type},"
        f"{get_response_elems(license_type)['totalResults']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_WRITE_FILE as specified in that constant.
    """
    license_list = get_license_list()
    for license_type in license_list:
        record_license_data(license_type)


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

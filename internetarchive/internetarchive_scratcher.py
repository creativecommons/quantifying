"""
This file is dedicated to obtain a .csv record report for internetarchive
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

# First-party/Local
from internetarchive.search import Search
from internetarchive.session import ArchiveSession

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


def get_license_list():
    """Provides the list of license from 2018's record of Creative Commons.
    Returns:
        np.array: An np array containing all license types that should be
        searched via Programmable Search Engine.
    """
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    license_list = cc_license_data[0].unique()
    return license_list


def get_response_elems(license, eb=False):
    """Provides the metadata for query of specified parameters
    Args:
        license:
            A string representing the type of license, and should be a segment
            of its URL towards the license description. Alternatively, the
            default None value stands for having no assumption about license
            type.
        eb:
            A boolean indicating whether there should be exponential callback.
            Is by default False.

    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    try:
        elem_session = ArchiveSession()
        search_data = Search(
            elem_session,
            f'/metadata/licenseurl:("http://creativecommons.org/{license}")',
        )
        search_data_dict = {"totalResults": len(search_data)}
        return search_data_dict
    except:
        if eb:
            expo_backoff()
            get_response_elems(license)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)


def set_up_data_file():
    """Writes the header row to file to contain IA data."""
    header_title = "LICENSE TYPE,Document Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{header_title}\n")


def record_license_data(license_type):
    """Writes the row for LICENSE_TYPE to file to contain IA Query data.
    Args:
        license:
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
    records these data into the DATA_wRITE_FILE as specified in that constant.
    """
    license_list = get_license_list()
    for license_type in license_list:
        record_license_data(license_type)
        print("DEBUG", f"Processed {license_type}")


def main():
    global DATA_WRITE_FILE
    today = dt.datetime.today()
    DATA_WRITE_FILE += (
        f"/data_internetarchive_{today.year}_{today.month}_{today.day}.txt"
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

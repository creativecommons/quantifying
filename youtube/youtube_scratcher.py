"""
This file is dedicated to obtain a .csv record report for Google Custom Search 
Data.
"""

# Standard library
import datetime as dt
import json
import os
import random
import sys
import time
import traceback

# Third-party
import query_secret
import requests

CWD = os.path.dirname(os.path.abspath(__file__))
CALLBACK_INDEX = 2
CALLBACK_EXPO = 0
MAX_WAIT = 64
DATA_WRITE_FILE = CWD
API_KEY = query_secret.API_KEY


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


def get_request_url():
    """Provides the API Endpoint URL for specified parameter combinations.

    Returns:
        string: A string representing the API Endpoint URL for the query
        specified by this function's parameters.
    """
    return (
        r"https://youtube.googleapis.com/youtube/v3/search?part=snippet"
        r"&type=video&videoLicense=creativeCommon"
        f"&key={API_KEY}"
    )


def get_response_elems(eb=False):
    """Provides the metadata for query of specified parameters
    Args:
        eb:
            A boolean indicating whether there should be exponential callback.
            Is by default False.
    Returns:
        dict: A dictionary mapping metadata to its value provided from the API
        query of specified parameters.
    """
    search_data = None
    try:
        url = get_request_url()
        search_data = requests.get(url).json()
        return search_data
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


def record_all_licenses():
    """Records the data of all license types findable in the license list and
    records these data into the DATA_wRITE_FILE as specified in that constant.
    """
    with open(DATA_WRITE_FILE, "a") as f:
        json.dump(get_response_elems(eb=True), f)


def main():
    # TODO
    global DATA_WRITE_FILE
    today = dt.datetime.today()
    DATA_WRITE_FILE += (
        f"/data_youtube_{today.year}_{today.month}_{today.day}.txt"
    )
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

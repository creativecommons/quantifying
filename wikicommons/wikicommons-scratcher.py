"""
This file is dedicated to obtain a .csv record report for WikiCommons
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

def get_content_request_url(license):
    """_summary_

    Args:
        license (_type_): _description_

    Returns:
        _type_: _description_
    """
    base_url = (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&prop=categoryinfo&titles="
        f"Category:{license}"
    )
    return base_url

def get_subcat_request_url(license):
    """_summary_

    Args:
        license (_type_): _description_

    Returns:
        _type_: _description_
    """
    base_url = (
        r"https://commons.wikimedia.org/w/api.php?"
        r"action=query&cmtitle="
        f"Category:{license}"
        r"&cmtype=subcat&list=categorymembers"
    )
    return base_url

def get_subcategories(license, eb=False):
    """_summary_

    Args:
        license (_type_): _description_
        eb (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    try:
        request_url = get_subcat_request_url(license)
        search_data = requests.get(request_url).json()
        cat_list = []
        for members in search_data["query"]["categorymembers"]:
            cat_list.append(members["title"])
        return cat_list
    except:
        if eb:
            expo_backoff()
            get_subcategories(license)
        elif "query" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)

def get_license_contents(license, eb=False):
    try:
        url = get_content_request_url(license)
        search_data = requests.get(url).json()
        file_cnt = 0
        page_cnt = 0
        for pages in search_data["query"]["pages"]:
            file_cnt += pages["categoryinfo"]["files"]
            page_cnt += pages["categoryinfo"]["pages"]
        search_data_dict = {
            "total_file_cnt": file_cnt,
            "total_page_cnt": page_cnt
        }
        return search_data_dict
    except:
        if eb:
            expo_backoff()
            get_license_contents(license)
        elif "queries" not in search_data:
            print(search_data)
            sys.exit(1)
        else:
            print("ERROR (1) Unhandled exception:", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)
            sys.exit(1)

def set_up_data_file():
    """Writes the header row to file to contain WikiCommons Query data."""
    header_title = "LICENSE TYPE,File Count, Page Count"
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(header_title + "\n")

def record_license_data(license_type):
    """_summary_

    Args:
        license_type (_type_): _description_
    """
    search_result = get_license_contents(license_type)
    data_log = (
        f"{license_type},"
        f"{search_result['files']},{search_result['pages']}"
    )
    with open(DATA_WRITE_FILE, "a") as f:
        f.write(f"{data_log}\n")

def recur_record_all_licenses(alias = "Free_Creative_Commons_licenses"):
    """_summary_

    Args:
        alias (str, optional): _description_. Defaults to "Free_Creative_Commons_licenses".
    """
    cur_cat = alias.split("/")[-1]
    subcategories = get_subcategories(cur_cat)
    record_license_data(cur_cat)
    for cats in subcategories:
        recur_record_all_licenses(alias = f"{alias}/{cats}")

def main():
    global DATA_WRITE_FILE
    today = dt.datetime.today()
    DATA_WRITE_FILE += (
        f"/data_wikicommons_{today.year}_{today.month}_{today.day}.txt"
    )
    set_up_data_file()
    recur_record_all_licenses()
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

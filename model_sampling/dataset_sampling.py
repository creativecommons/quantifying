#!/usr/bin/env python
"""
This file is a Python script for generating the Data Science Discovery modeling
task's training dataset.
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
import sqlalchemy
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEYS = query_secrets.API_KEYS
API_KEYS_IND = 0
CWD = os.path.dirname(os.path.abspath(__file__))
MODEL_DATABASE = (
    f"{CWD}"
    f"/model_dataset_less_constraints.db"
)
PSE_KEY = query_secrets.PSE_KEY

'''
RIGHTS_MAP = {
    "by": "cc_attribute",
    "sa": "cc_sharealike",
    "nc": "cc_noncommercial",
    "nd": "cc_nonderived",
    "publicdomain": "cc_publicdomain"
}


def get_rights(license_type):
    #TODO: Documentation
    return [
        RIGHTS_MAP[right]
        for right in RIGHTS_MAP
        if right in license_type
    ]
'''

def get_license_map():
    #TODO: Documentation
    cc_license_data = pd.read_csv(f"{CWD}/legal-tool-paths.txt", header=None)
    license_pattern = r"((?:[^/]+/){2}(?:[^/]+)).*"
    license_pattern_map = {
        "by": "licenses/by/",
        "by-sa": "licenses/by-sa/",
        "by-nc": "licenses/by-nc/",
        "by-nc-sa": "licenses/by-nc-sa/",
        "by-nd": "licenses/by-nd/",
        "by-nc-nd": "licenses/by-nc-nd/|licenses/by-nd-nc/",
        "publicdomain": "publicdomain/"
    }
    license_list = pd.Series(
        cc_license_data[0]
        .str.extract(license_pattern, expand=False)
        .dropna()
        .unique()
    )
    license_series_map = {
        k: license_list[license_list.str.contains(license_pattern_map[k])]
        for k in license_pattern_map
    }
    return license_series_map

def get_api_endpoint(license_type, license_rights, start):
    #TODO: Documentation
    try:
        api_key = API_KEYS[API_KEYS_IND]
        base_url = (
            r"https://customsearch.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={PSE_KEY}&"
            f"q=-fileType%3Apdf%20-inurl%3Apdf%20-pdf&"
            f"start={start}&"
        )
        base_url = (
            f"{base_url}&linkSite=creativecommons.org"
            f'{license_type.replace("/", "%2F")}'
            #f"&rights={'%7'.join(license_rights)}"
        )
        return base_url
    except Exception as e:
        if isinstance(e, IndexError):
            print(
                "IndexError: Depleted all API Keys provided",
                file=sys.stderr
            )
        else:
            raise e

def get_api_response(license_type, start, retry_on_empty = 2):
    #TODO: Documentation
    try:
        request_url = get_api_endpoint(
            license_type,
            None, #get_rights(license_type),
            start
        )
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
        return search_data["items"]
    except Exception as e:
        if isinstance(e, KeyError):
            if retry_on_empty:
                return get_api_response(license_type, start, retry_on_empty - 1)
            else:
                return {}
        if isinstance(e, requests.exceptions.HTTPError):
            global API_KEYS_IND
            API_KEYS_IND += 1
            print(
                "Changing API KEYS due to depletion of quota", file=sys.stderr
            )
            return get_api_response(license_type, start)
        else:
            print(f"Request URL was {request_url}", file=sys.stderr)
            raise e

def get_address_entries(web_url, content_char_count=5000):
    #TODO: Documentation
    try:
        web_contents = requests.get(web_url).text
        encoding = EncodingDetector.find_declared_encoding(
            web_contents,
            is_html = True
        )
        soup = BeautifulSoup(web_contents, "lxml", from_encoding=encoding)
        for script in soup(["script", "style"]):
            script.extract()
        parse_result = soup.get_text("", strip = True)
        return (web_url, soup.title, parse_result[:content_char_count])
    except Exception as e:
        return None

def get_license_type_sample_df(license_type):
    #TODO: Documentation
    license_sample_dict = {
        "license": [],
        "url": [],
        "title": [],
        "contents": []
    }
    for start_ind in range(1, 101, 10):
        license_subresponse = get_api_response(license_type, start_ind)
        for entry in license_subresponse:
            if ".pdf" in entry["link"] or ".txt" in entry["link"]:
                continue
            address_entries = get_address_entries(
                entry["link"]
            )
            if address_entries is not None:
                license_sample_dict["license"].append(license_type)
                license_sample_dict["url"].append(address_entries[0])
                license_sample_dict["title"].append(str(address_entries[1]))
                license_sample_dict["contents"].append(address_entries[2])
    print(f"DEBUG: {license_type} has been sampled.")
    return pd.DataFrame(license_sample_dict)

def get_license_series_sample_df(general_license_series):
    #TODO: Documentation
    return pd.concat(
        [
            get_license_type_sample_df(license_type)
            for license_type in general_license_series
        ]
    )

def load_general_licenses():
    #TODO: Documentation
    engine = sqlalchemy.create_engine(f"sqlite:///{CWD}/modeling_dataset.db")
    engine.connect()
    license_map = get_license_map()
    for general_type in license_map:
        sampled_df = get_license_series_sample_df(license_map[general_type])
        sampled_df.to_sql(general_type, engine, if_exists = 'replace')

def main():
    load_general_licenses()


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

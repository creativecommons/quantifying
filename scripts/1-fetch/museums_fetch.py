#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from the Museums Victoria Collections API.
"""

# Standard library
import argparse
import csv
import json
import os
import sys
import textwrap
import traceback

# Third-party
import requests
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
FILE_RECORDS = os.path.join(PATHS["data_phase"], "museums_raw.csv")
HEADER_RECORDS = [
    "ID",
    "TITLE",
    "RECORD TYPE",
    "CONTENT LICENCE SHORT NAME",
    "MEDIA JSON",
]
QUARTER = os.path.basename(PATHS["data_quarter"])
BASE_URL = "https://collections.museumsvictoria.com.au/api/search"
RECORD_TYPES = [
    "article",
    "item",
    "species",
    "specimen",
]  # Type of record to return
MAX_PER_PAGE = 100  # Pagination limit as defined by the API documentation


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_requests_session():
    """
    Returns a configured requests session with retries and a User-Agent.
    """
    max_retries = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    # Museums Victoria API requires a User-Agent header
    session.headers.update({"User-Agent": shared.USER_AGENT})
    return session


def sanitize_string(s):
    """Replaces newline and carriage return characters with a space."""
    if isinstance(s, str):
        return s.replace("\n", " ").replace("\r", "")
    return s


def initialize_data_file(file_path, header):
    if not os.path.isfile(file_path):
        with open(file_path, "w", newline="\n", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=header, dialect="unix"
            )
            writer.writeheader()


def write_data(args, data):
    """
    Saves the fetched records to a CSV file.
    """
    if not args.enable_save:
        return args
    LOGGER.info("Saving fetched data")
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    for record in data:
        media = record.get("media")
        media_json_string = json.dumps(
            [
                {"type": i.get("type"), "licence": i.get("licence")}
                for i in media
            ]
        )
        content_license_short_name = record.get("licence", {}).get(
            "shortName", "Not Found"
        )
        row = {
            "ID": record.get("id"),
            "TITLE": record.get("title"),
            "RECORD TYPE": record.get("recordType"),
            "CONTENT LICENCE SHORT NAME": sanitize_string(
                content_license_short_name
            ),
            "MEDIA JSON": sanitize_string(media_json_string),
        }
        initialize_data_file(FILE_RECORDS, HEADER_RECORDS)
        with open(FILE_RECORDS, "a", newline="\n", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file, fieldnames=HEADER_RECORDS, dialect="unix"
            )
            writer.writerow(row)
        LOGGER.info(f"Successfully saved records to {FILE_RECORDS}")

    return args


def fetch_museums_victoria_data(args, session):
    """
    Fetches all records with images from the Museums Victoria API by iterating
    through all record types and handling pagination.
    """

    # Iterate through each record type
    for record_type in RECORD_TYPES:
        current_page = 1
        total_pages = None

        LOGGER.info(f"--- Starting fetch for: {record_type.upper()} ---")

        while True:
            # 1. Construct the API query parameters
            params = {
                "recordtype": record_type,
                # "perpage": 20,
                "perpage": MAX_PER_PAGE,
                "page": current_page,
                # "page": 1,
                "envelope": "true",
            }
            try:
                r = session.get(BASE_URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                results = data.get("response", [])
            except requests.HTTPError as e:
                raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
            except requests.RequestException as e:
                raise shared.QuantifyingException(f"Request Exception: {e}", 1)
            except KeyError as e:
                raise shared.QuantifyingException(f"KeyError: {e}", 1)

                # 3. Handle data and pagination metadata
            write_data(args, results)

            # Initialize total_pages on the first request for this record type
            if total_pages is None:
                headers = data.get("headers", {})
                total_pages = int(headers.get("totalResults", "0"))

            # 4. Check for next page and break the loop if done
            current_page += 1
            if current_page > total_pages:
                break


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    fetch_museums_victoria_data(args, get_requests_session())
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Museums Victoria data for {QUARTER}",
    )
    shared.git_push_changes(args, PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.critical(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)

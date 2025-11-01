#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from the Museums Victoria Collections API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
from collections import defaultdict

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
BASE_URL = "https://collections.museumsvictoria.com.au/api/search"
FILE1_COUNT = shared.path_join(
    PATHS["data_phase"], "museums_victoria_1_count.csv"
)
FILE2_MEDIA = shared.path_join(
    PATHS["data_phase"], "museums_victoria_2_count_by_media.csv"
)
FILE3_RECORD = shared.path_join(
    PATHS["data_phase"], "museums_victoria_3_count_by_record.csv"
)
HEADER1_COUNT = ["TOOL IDENTIFIER", "COUNT"]
HEADER2_MEDIA = ["TOOL IDENTIFIER", "MEDIA TYPE", "COUNT"]
HEADER3_RECORD = ["TOOL IDENTIFIER", "RECORD TYPE", "COUNT"]
MAX_PER_PAGE = 100  # Pagination limit as defined by the API documentation
QUARTER = os.path.basename(PATHS["data_quarter"])
RECORD_TYPES = [
    "article",
    "item",
    "species",
    "specimen",
]  # Type of record to return


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
        with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=header, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    if not args.enable_save:
        return

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    initialize_data_file(FILE1_COUNT, HEADER1_COUNT)
    initialize_data_file(FILE2_MEDIA, HEADER2_MEDIA)
    initialize_data_file(FILE3_RECORD, HEADER3_RECORD)


def write_counts_to_csv(args, data: dict):
    if not args.enable_save:
        return
    for data in data.items():
        rows = []
        file_path = data[0]
        if file_path == FILE2_MEDIA:
            fieldnames = HEADER2_MEDIA
            for media_type in data[1].items():
                rows.extend(
                    {
                        "TOOL IDENTIFIER": row[0],
                        "MEDIA TYPE": media_type[0],
                        "COUNT": row[1],
                    }
                    for row in media_type[1].items()
                )
        elif file_path == FILE3_RECORD:
            fieldnames = HEADER3_RECORD
            for record_type in data[1].items():
                rows.extend(
                    {
                        "TOOL IDENTIFIER": row[0],
                        "RECORD TYPE": record_type[0],
                        "COUNT": row[1],
                    }
                    for row in record_type[1].items()
                )
        else:
            fieldnames = HEADER1_COUNT
            rows = [
                {
                    "TOOL IDENTIFIER": row[0],
                    "COUNT": row[1],
                }
                for row in data[1].items()
            ]
        with open(file_path, "a", encoding="utf-8", newline="\n") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=fieldnames, dialect="unix"
            )
            writer.writerows(rows)


def fetch_museums_victoria_data(session):
    """
    Fetches all records with images from the Museums Victoria API by iterating
    through all record types and handling pagination.
    """

    record_counts = defaultdict(lambda: defaultdict(int))
    media_counts = defaultdict(lambda: defaultdict(int))
    licences_count = defaultdict(int)

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
            except requests.HTTPError as e:
                raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
            except requests.RequestException as e:
                raise shared.QuantifyingException(f"Request Exception: {e}", 1)
            except KeyError as e:
                raise shared.QuantifyingException(f"KeyError: {e}", 1)
            data = r.json()
            results = data.get("response", [])
            for res in results:
                media_list = res.get("media", [])
                for media_item in media_list:
                    licence_data = media_item.get("licence")

                    # COUNTING THE UNIQUE LICENCE TYPES
                    license_short_name = licence_data.get("shortName")
                    if license_short_name:
                        licences_count[license_short_name] += 1

                    # COUNTING LICENSES BY MEDIA TYPES
                    media_type = media_item.get("type")
                    media_counts[media_type][license_short_name] += 1

                    # COUNTING LICENSES BY RECORD TYPES
                    record_counts[record_type][license_short_name] += 1
            if total_pages is None:
                headers = data.get("headers", {})
                # total_pages = 1
                total_pages = int(headers.get("totalResults", "0"))

            current_page += 1
            if current_page > total_pages:
                break
    return {
        FILE1_COUNT: licences_count,
        FILE2_MEDIA: media_counts,
        FILE3_RECORD: record_counts,
    }


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    session = get_requests_session()
    data = fetch_museums_victoria_data(session)
    write_counts_to_csv(args, data)
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

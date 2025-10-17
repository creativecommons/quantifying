#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from Openverse API.
"""

# Standard library
import argparse
import csv
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

LOGGER.info("Starting Openverse Fetch Script...")


# Constants
FILE_PATH = os.path.join(PATHS["data_phase"], "openverse_fetch.csv")
OPENVERSE_FIELDS = [
    "source",
    "media_type",
    "license",
    "license_version",
    "media_count",
]
OPENVERSE_BASE_URL = "https://api.openverse.org/v1"
MEDIA_TYPES = ["audio", "images"]
PAGE_SIZE = 20  # API limit for anonymous requests


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
    max_retries = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=shared.RETRY_STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update(
        {"accept": "application/json", "User-Agent": shared.USER_AGENT}
    )
    return session


def query_openverse(session):
    """
    Fetch records from Openverse API.
    """
    tally = {}
    for media_type in MEDIA_TYPES:
        LOGGER.info(f"Fetching {media_type} data...")
        url = f"{OPENVERSE_BASE_URL}/{media_type}/?page_size={PAGE_SIZE}"
        try:
            response = session.get(url)
            if response.status_code == 401:
                raise shared.QuantifyingException(
                    f"Unauthorized(401): Check API key for {media_type}.",
                    exit_code=1,
                )
            response.raise_for_status()
            data = response.json()
            records = data.get("results", [])
            for record in records:
                key = (
                    record.get(OPENVERSE_FIELDS[0], ""),
                    media_type,
                    record.get(OPENVERSE_FIELDS[2], ""),
                    record.get(OPENVERSE_FIELDS[3], ""),
                )
                # key = ("source", "media_type", "license", "license_version")
                tally[key] = tally.get(key, 0) + 1
        except requests.RequestException as e:
            LOGGER.error(f"Openverse fetch failed: {e}")
            raise shared.QuantifyingException(f"Openverse fetch failed: {e}")
    # Convert tally dictionary to a list of dicts for writing
    aggregate = [
        {
            OPENVERSE_FIELDS[0]: field[0],
            "media_type": field[1],
            OPENVERSE_FIELDS[2]: field[2],
            OPENVERSE_FIELDS[3]: field[3],
            "media_count": count,
        }
        for field, count in tally.items()
    ]
    return aggregate


def write_data(args, data):
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    with open(FILE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OPENVERSE_FIELDS)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main():
    args = parse_arguments()
    session = get_requests_session()
    records = query_openverse(session)
    write_data(args, records)
    LOGGER.info(f"Fetched {len(records)} unique Openverse records")


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

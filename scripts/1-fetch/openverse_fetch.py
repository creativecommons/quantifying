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
from shared import RETRY_STATUS_FORCELIST  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
FILE_PATH = os.path.join(PATHS["data_phase"], "openverse_fetch.csv")
OPENVERSE_FIELDS = ["id", "title", "creator", "license"]


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
        status_forcelist=RETRY_STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update({"accept": "application/json"})
    return session


def query_openverse(session, page_size=10):
    """
    Fetch a small number of works from Openverse API.
    """
    url = f"https://api.openverse.engineering/v1/images/?page_size={page_size}"
    try:
        response = session.get(url)
        if response.status_code == 401:
            raise shared.QuantifyingException(
                "Unauthorized(401): Check API key.", exit_code=1
            )
        response.raise_for_status()
        data = response.json()
        works = data.get("results", [])
        extracted = []
        for work in works:
            extracted.append(
                {field: work.get(field, "") for field in OPENVERSE_FIELDS}
            )
        return extracted
    except requests.RequestException as e:
        LOGGER.error(f"Openverse fetch failed: {e}")
        raise shared.QuantifyingException(f"Openverse fetch failed: {e}")


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
    works = query_openverse(session)
    write_data(args, works)
    LOGGER.info(f"Fetched {len(works)} Openverse works")


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

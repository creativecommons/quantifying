#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from Wikipedia API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
import random
# Third-party
import requests
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
WIKI_BASE_URL = "https://en.wikipedia.org/w/api.php"
# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)
FILE1_COUNT = os.path.join(PATHS["data_phase"], "wiki_1_count.csv")
HEADER1_COUNT = ["PLAN_INDEX", "TOOL_IDENTIFIER", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])
WIKIPEDIA_RETRY_STATUS_FORCELIST = [
    408,  # Request Timeout
    422,  # Unprocessable Content (Validation failed, or endpoint spammed)
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
]

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
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: avoid hitting API (generate fake data)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args

def get_requests_session():
    max_retries = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=WIKIPEDIA_RETRY_STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update({"User-Agent": "quantifying-wikipedia-fetch/1.0 (contact@example.com)"})
    return session


def write_data(args, tool_data):
    if not args.enable_save:
        return args

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    with open(FILE1_COUNT, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER1_COUNT, dialect="unix"
        )
        writer.writeheader()
        for row in tool_data:
            writer.writerow(row)
    return args


def query_wikipedia(args, session):
    LOGGER.info("Fetching Wikipedia rightsinfo + article count")
    tool_data = []

    try:
        if args.dev:
            license_name = "Creative Commons (DEV)"
            article_count = random.randint(100000, 5000000)
        else:
            params = {
                "action": "query",
                "meta": "siteinfo",
                "siprop": "general|statistics|rightsinfo",
                "format": "json",
            }
            r = session.get(WIKI_BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            stats = data["query"]["statistics"]
            rights = data["query"]["rightsinfo"]

            license_name = rights.get("text", "")
            license_url = rights.get("url", "")
            article_count = stats.get("articles", 0)

        tool_data.append({
            "PLAN_INDEX": 1,
            "TOOL_IDENTIFIER": f"{license_name}",
            "COUNT": article_count
        })

        LOGGER.info(f"License: {license_name} -> Articles: {article_count}")

    except requests.RequestException as e:
        LOGGER.error(f"Request error while fetching Wikipedia rightsinfo: {e}")
        raise shared.QuantifyingException(f"Request error: {e}", 1)

    return tool_data

def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    tool_data = query_wikipedia(args, get_requests_session())
    args = write_data(args, tool_data)
    args = shared.git_add_and_commit(args, PATHS["repo"], PATHS["data_quarter"], f"Add and commit new Wikipedia data for {QUARTER}")
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
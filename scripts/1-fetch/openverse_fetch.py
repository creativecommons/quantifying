#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from Openverse API.

Note:
    Because anonymous Openverse API access
    returns a maximum of ~240 result count
    per source-license combination, this
    script currently provides approximate counts.
    It does not include pagination or license_version
    breakdown.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
import urllib

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
FILE_PATH = os.path.join(PATHS["data_phase"], "openverse_fetch.csv")
MEDIA_TYPES = ["audio", "images"]
OPENVERSE_BASE_URL = "https://api.openverse.org/v1"
OPENVERSE_FIELDS = [
    "SOURCE",
    "MEDIA_TYPE",
    "LICENSE",
    "MEDIA_COUNT",
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
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_requests_session():
    max_retries = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update(
        {"accept": "application/json", "User-Agent": shared.USER_AGENT}
    )
    return session


def get_all_sources_and_licenses(session, media_type):
    """
    Fetch all available sources for a given media_type.
    """
    LOGGER.info(f"Fetching all sources for {media_type}")
    url = f"{OPENVERSE_BASE_URL}/{media_type}/stats/?format=json"
    # Standard /stats/ license
    licenses = [
        "by",
        "by-nc",
        "by-nc-nd",
        "by-nc-sa",
        "by-nd",
        "by-sa",
        "cc0",
        "nc-sampling+",
        "pdm",
        "sampling+",
    ]
    try:
        response = session.get(url)
        response.raise_for_status()
        records = response.json()
        raw_sources = sorted(
            [
                record["source_name"]
                for record in records
                if "source_name" in record
            ]
        )
        """
        To ensure the sources in /stats/ endpoints are truly
        indexed in Openverse's catalog.
        """
        valid_sources = set()
        for source in raw_sources:
            new_response = session.get(
                f"{OPENVERSE_BASE_URL}/{media_type}/?"
                f"source={source}&format=json"
            )
            if new_response.status_code == 200:
                valid_sources.add(source)
            else:
                LOGGER.info(
                    f"Skipping source {source}: "
                    f"not available in /{media_type}/ endpoint"
                )
        LOGGER.info(f"Found {len(valid_sources)} sources for {media_type}")
        return valid_sources, set(licenses)
    except (requests.HTTPError, requests.RequestException) as e:
        LOGGER.error(f"Failed to fetch sources and licenses: {e}")
        raise shared.QuantifyingException(
            f"Failed to fetch sources and licenses: {e}"
        )


def query_openverse(session):
    """
    Fetch available sources given the media_type and use
    standard list of Openverse's standard licenses.
    """
    tally = {}
    for media_type in MEDIA_TYPES:
        LOGGER.info(f"FETCHING {media_type.upper()} DATA...")
        sources, licenses = get_all_sources_and_licenses(session, media_type)
        for source in sources:
            for license in licenses:
                url = (
                    f"{OPENVERSE_BASE_URL}/{media_type}/?"
                    # encode the license
                    f"source={source}&"
                    f"license={urllib.parse.quote(license, safe='')}"
                    "&format=json&page=1"
                )
                LOGGER.info(f"Target URL: {url}")
                try:
                    response = session.get(url)
                    if response.status_code == 401:
                        raise shared.QuantifyingException(
                            "Unauthorized(401): Check API key for"
                            f" {media_type}.",
                            exit_code=1,
                        )
                    response.raise_for_status()
                    data = response.json()
                    count = data.get("result_count", 0)
                    key = (source, media_type, license)
                    tally[key] = count
                except (requests.HTTPError, requests.RequestException) as e:
                    LOGGER.error(f"Openverse fetch failed: {e}")
                    raise shared.QuantifyingException(
                        f"Openverse fetch failed: {e}"
                    )
    # Convert tally dictionary to a list of dicts for writing
    LOGGER.info("Aggregating the data")
    aggregate = [
        {
            OPENVERSE_FIELDS[0].lower(): field[0],  # SOURCE
            OPENVERSE_FIELDS[1].lower(): field[1],  # MEDIA_TYPE
            OPENVERSE_FIELDS[2].lower(): (
                f"{'cc ' + field[2] if field[2] not in ['pdm', 'cc0'] else field[2]}"  # noqa: E501
            ),  # LICENSE
            OPENVERSE_FIELDS[3].lower(): count,  # MEDIA_COUNT
        }
        for field, count in tally.items()
    ]
    return aggregate


def write_data(args, data):
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    with open(FILE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OPENVERSE_FIELDS,
            dialect="unix",
        )
        writer.writeheader()
        for row in data:
            writer.writerow({key.upper(): value for key, value in row.items()})


def main():
    args = parse_arguments()
    session = get_requests_session()
    LOGGER.info("Starting Openverse Fetch Script...")
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

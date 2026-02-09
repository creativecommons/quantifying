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
    "TOOL_IDENTIFIER",
    "MEDIA_COUNT",
]
OPENVERSE_LEGAL_TOOLS = [
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


def get_all_sources_and_licenses(session, media_type):
    """
    Fetch all available sources for a given media_type.
    """
    LOGGER.info(f"Fetching all sources for the /{media_type}/ endpoint")
    url = f"{OPENVERSE_BASE_URL}/{media_type}/stats/?format=json"
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
                LOGGER.warning(
                    f"Skipping source {source}:"
                    f" not available in /{media_type}/ endpoint"
                )
        LOGGER.info(
            f"Found {len(valid_sources)} valid sources for {media_type}"
        )
        return valid_sources, set(OPENVERSE_LEGAL_TOOLS)
    except (requests.HTTPError, requests.RequestException) as e:
        raise shared.QuantifyingException(
            f"Failed to fetch sources and licenses: {e}", exit_code=1
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
        for source_name in sources:
            for license in licenses:
                # encode the license to escape '+' e.g sampling+
                encoded_license = urllib.parse.quote(license, safe="")
                url = (
                    f"{OPENVERSE_BASE_URL}/{media_type}/?"
                    f"source={source_name}&"
                    f"license={encoded_license}"
                    "&format=json&page=1"
                )
                LOGGER.info(
                    "Fetching Openverse data:"
                    f" media_type={media_type} |"
                    f" source={source_name} |"
                    f" license={license}"
                )
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
                    # Skip (source x license) with result_count = 0
                    if count > 0:
                        key = (source_name, media_type, license)
                        tally[key] = count
                    else:
                        LOGGER.warning(
                            f"Skipping ({source_name}, {license}): count is 0"
                        )
                except (requests.HTTPError, requests.RequestException) as e:
                    raise shared.QuantifyingException(
                        f"Openverse fetch failed: {e}", exit_code=1
                    )
    LOGGER.info("Aggregating the data")
    aggregate = []
    for (source, media_type, license_code), media_count in tally.items():
        # Append prefix "cc" except for 'pdm' and 'cc0'
        if license_code not in ["pdm", "cc0"]:
            tool_identifier = f"cc {license_code}"
        else:
            tool_identifier = license_code
        aggregate.append(
            {
                OPENVERSE_FIELDS[0]: source,
                OPENVERSE_FIELDS[1]: media_type,
                OPENVERSE_FIELDS[2]: tool_identifier.upper(),
                OPENVERSE_FIELDS[3]: media_count,
            }
        )
    return aggregate


def main():
    args = parse_arguments()
    LOGGER.info("Starting Openverse Fetch Script...")
    session = shared.get_session(accept_header="application/json")
    records = query_openverse(session)
    shared.rows_to_csv(args, FILE_PATH, OPENVERSE_FIELDS, records)
    LOGGER.info(f"Fetched {len(records)} unique Openverse records.")


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

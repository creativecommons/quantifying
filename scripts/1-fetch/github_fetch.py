#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from GitHub API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
import urllib.parse

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
FILE_COUNT = os.path.join(PATHS["data_phase"], "github_1_count.csv")
GH_TOKEN = os.getenv("GH_TOKEN")
# Also see: https://en.wikipedia.org/wiki/Public-domain-equivalent_license
GITHUB_TOOLS = [
    {"TOOL_IDENTIFIER": "BSD Zero Clause License", "SPDX_IDENTIFIER": "0BSD"},
    {"TOOL_IDENTIFIER": "CC0 1.0", "SPDX_IDENTIFIER": "CC0-1.0"},
    {"TOOL_IDENTIFIER": "CC BY 4.0", "SPDX_IDENTIFIER": "CC-BY-4.0"},
    {"TOOL_IDENTIFIER": "CC BY-SA 4.0", "SPDX_IDENTIFIER": "CC-BY-SA-4.0"},
    {"TOOL_IDENTIFIER": "MIT No Attribution", "SPDX_IDENTIFIER": "MIT-0"},
    {"TOOL_IDENTIFIER": "Unlicense", "SPDX_IDENTIFIER": "Unlicense"},
    {"TOOL_IDENTIFIER": "Total public repositories", "SPDX_IDENTIFIER": "N/A"},
]
HEADER_COUNT = ["TOOL_IDENTIFIER", "SPDX_IDENTIFIER", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])


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


def check_for_completion():
    try:
        with open(FILE_COUNT, "r", encoding="utf-8") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) == len(GITHUB_TOOLS):
                raise shared.QuantifyingException(
                    f"Data fetch completed for {QUARTER}", 0
                )
    except FileNotFoundError:
        pass  # File may not be found without --enable-save, etc.


def query_github(args, session):
    tool_data = []
    for tool in GITHUB_TOOLS:
        tool_identifier = tool["TOOL_IDENTIFIER"]
        spdx_identifier = tool["SPDX_IDENTIFIER"]
        LOGGER.info(f"Query: tool: {tool_identifier}, spdx: {spdx_identifier}")

        base_url = "https://api.github.com/search/repositories?per_page=1&q="
        search_parameters = "is:public"
        if tool_identifier != "Total public repositories":
            search_parameters = (
                f"{search_parameters} license:{spdx_identifier.lower()}"
            )
        search_parameters = urllib.parse.quote(search_parameters, safe=":/")
        request_url = f"{base_url}{search_parameters}"

        try:
            with session.get(request_url) as response:
                response.raise_for_status()
                search_data = response.json()
                count = search_data["total_count"]
            tool_data.append(
                {
                    "TOOL_IDENTIFIER": tool_identifier,
                    "SPDX_IDENTIFIER": spdx_identifier,
                    "COUNT": count,
                }
            )
            LOGGER.info(f"count: {count}")
        except requests.HTTPError as e:
            raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
        except requests.RequestException as e:
            raise shared.QuantifyingException(f"Request Exception: {e}", 1)
        except KeyError as e:
            raise shared.QuantifyingException(f"KeyError: {e}", 1)
    return tool_data


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    check_for_completion()
    session = shared.get_session(
        accept_header="application/vnd.github+json",
    )
    if GH_TOKEN:
        session.headers.update({"authorization": f"Bearer {GH_TOKEN}"})

    tool_data = query_github(args, session)
    if len(tool_data) < len(GITHUB_TOOLS):
        LOGGER.error("Unable to fetch all records. Aborting.")
        return args
    shared.rows_to_csv(args, FILE_COUNT, HEADER_COUNT, tool_data)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new GitHUB data for {QUARTER}",
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

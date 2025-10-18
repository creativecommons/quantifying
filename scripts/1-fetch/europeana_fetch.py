#!/usr/bin/env python
"""
Fetch high-level Europeana statistics for Quantifying the Commons.
Aggregates data by DATA_PROVIDER, LEGAL_TOOL, and COUNT.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
from collections import defaultdict

# Third-party
import requests
from dotenv import load_dotenv
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Constants
EUROPEANA_API_KEY = os.getenv("EUROPEANA_API_KEY")
BASE_URL = "https://api.europeana.eu/record/v2/search.json"
FILE_STATS = shared.path_join(PATHS["data_phase"], "europeana_1_count.csv")
HEADER_STATS = ["DATA_PROVIDER", "LEGAL_TOOL", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

# Log the start of script execution
LOGGER.info("Europeana high-level stats script execution started.")


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limit number of results to fetch (default: 100).",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving aggregated results to CSV.",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, push).",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def initialize_data_file(file_path, header):
    """Initialize the data file with a header if it doesn't exist."""
    if not os.path.isfile(file_path):
        with open(file_path, "w", newline="") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=header, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Ensure data directories and files exist."""
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    initialize_data_file(FILE_STATS, HEADER_STATS)


def fetch_europeana_data(args):
    """
    Fetch and aggregate data from the Europeana Search API
    by DATA_PROVIDER and LEGAL_TOOL.
    """
    LOGGER.info("Fetching aggregated Europeana data.")

    if not EUROPEANA_API_KEY:
        raise shared.QuantifyingException(
            "EUROPEANA_API_KEY not found in environment variables", 1
        )

    # Try different queries to get diverse content
    queries = ["art", "history", "science", "music", "photography"]
    items_per_query = max(20, args.limit // len(queries))
    all_items = []

    for query in queries:
        params = {
            "wskey": EUROPEANA_API_KEY,
            "rows": min(items_per_query, 20),
            "profile": "rich",
            "query": query,
        }

        try:
            LOGGER.info(
                f"Fetching {params['rows']} records for query: '{query}'"
            )
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            results = response.json()
            items = results.get("items", [])
            all_items.extend(items)
            LOGGER.info(f"Retrieved {len(items)} items for '{query}'")
            time.sleep(1)  # Be nice to the API
        except requests.RequestException as e:
            LOGGER.warning(f"Failed to fetch data for query '{query}': {e}")
            continue

    if not all_items:
        LOGGER.error("No items retrieved from any query")
        return []

    LOGGER.info(f"Total items retrieved: {len(all_items)}")

    # Aggregate by data provider and legal tool
    aggregation = defaultdict(lambda: defaultdict(int))

    for item in all_items:
        # Handle dataProvider (can be array or string)
        data_providers = item.get("dataProvider", [])
        if isinstance(data_providers, str):
            data_provider = data_providers
        elif data_providers and isinstance(data_providers, list):
            data_provider = data_providers[0] if data_providers else "Unknown"
        else:
            data_provider = "Unknown"

        # Handle rights/license information - extract only the license code
        rights = item.get("rights", [])
        if isinstance(rights, str):
            legal_tool = rights
        elif rights and isinstance(rights, list):
            legal_tool = rights[0] if rights else "Unknown"
        else:
            legal_tool = "Unknown"

        # Simplify legal tool (e.g., extract 'by/4.0/' → 'CC BY 4.0')
        if (
            legal_tool
            and legal_tool != "Unknown"
            and legal_tool.startswith("http")
        ):
            parts = legal_tool.strip("/").split("/")
            last_parts = parts[-2:]  # e.g., ['by', '4.0'] or ['InC', '1.0']
            if last_parts:
                # Join neatly with spaces and add CC if
                # it’s a Creative Commons license
                joined = " ".join(part.upper() for part in last_parts if part)
                if "creativecommons.org" in legal_tool:
                    legal_tool = f"CC {joined}"
                else:
                    legal_tool = joined
            else:
                legal_tool = "Unknown"

        aggregation[data_provider][legal_tool] += 1

    # Convert to flat list
    output = []
    for provider, licenses in aggregation.items():
        for legal_tool, count in licenses.items():
            output.append(
                {
                    "DATA_PROVIDER": provider,
                    "LEGAL_TOOL": legal_tool,
                    "COUNT": count,
                }
            )

    LOGGER.info(
        f"Aggregated data into {len(output)} provider-license combinations"
    )
    return output


def save_to_csv(args, data):
    """Save aggregated data to CSV."""
    if not args.enable_save:
        LOGGER.info("Save disabled - skipping file write")
        return
    if not data:
        LOGGER.warning("No data to save")
        return

    with open(FILE_STATS, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_STATS, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    LOGGER.info(f"Saved {len(data)} aggregated rows to {FILE_STATS}.")


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)

    data = fetch_europeana_data(args)
    save_to_csv(args, data)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Europeana high-level statistics for {QUARTER}",
    )
    shared.git_push_changes(args, PATHS["repo"])

    LOGGER.info("Europeana high-level stats script completed successfully.")


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

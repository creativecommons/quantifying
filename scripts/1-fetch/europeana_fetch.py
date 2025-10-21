#!/usr/bin/env python
"""
Fetch high-level Europeana statistics for Quantifying the Commons.
Generates two datasets:
1) Without themes (aggregated by DATA_PROVIDER, LEGAL_TOOL)
2) With all themes (aggregated by DATA_PROVIDER, LEGAL_TOOL, THEME)
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
from requests.adapters import HTTPAdapter, Retry

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
FILE_WITH_THEMES = shared.path_join(
    PATHS["data_phase"], "europeana_with_themes.csv"
)
FILE_WITHOUT_THEMES = shared.path_join(
    PATHS["data_phase"], "europeana_without_themes.csv"
)
HEADER_WITH_THEMES = ["DATA_PROVIDER", "LEGAL_TOOL", "THEME", "COUNT"]
HEADER_WITHOUT_THEMES = ["DATA_PROVIDER", "LEGAL_TOOL", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

# Log start
LOGGER.info("Europeana dual-fetch (with & without themes) script started.")


def parse_arguments():
    """Parse command-line options."""
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


def get_requests_session():
    """Create a requests session with retry and headers."""
    max_retries = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update(
        {
            "accept": "application/json",
            "User-Agent": shared.USER_AGENT,
        }
    )
    return session


def fetch_europeana_data_without_themes(args):
    """Fetch and aggregate Europeana data without specifying themes."""
    LOGGER.info("Fetching Europeana data without themes.")

    if not EUROPEANA_API_KEY:
        raise shared.QuantifyingException(
            "EUROPEANA_API_KEY not found in environment variables", 1
        )

    params = {
        "wskey": EUROPEANA_API_KEY,
        "rows": args.limit,
        "profile": "rich",
        "query": "*",
    }

    session = get_requests_session()
    try:
        with session.get(BASE_URL, params=params, timeout=30) as response:
            response.raise_for_status()
            results = response.json()
            items = results.get("items", [])
    except requests.RequestException as e:
        LOGGER.error(f"Failed to fetch data without themes: {e}")
        return []

    LOGGER.info(f"Retrieved {len(items)} items without themes.")

    # --- Aggregate by DATA_PROVIDER + LEGAL_TOOL ---
    aggregation = defaultdict(lambda: defaultdict(int))

    for item in items:
        data_providers = item.get("dataProvider", [])
        data_provider = (
            data_providers
            if isinstance(data_providers, str)
            else (
                data_providers[0]
                if isinstance(data_providers, list) and data_providers
                else "Unknown"
            )
        )

        rights = item.get("rights", [])
        legal_tool = (
            rights
            if isinstance(rights, str)
            else (
                rights[0] if isinstance(rights, list) and rights else "Unknown"
            )
        )

        # Simplify license format if it’s a Creative Commons URL
        if (
            legal_tool
            and isinstance(legal_tool, str)
            and legal_tool.startswith("http")
        ):
            parts = legal_tool.strip("/").split("/")
            last_parts = parts[-2:]
            if last_parts:
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

    LOGGER.info(f"Aggregated data without themes into {len(output)} records.")
    return output


def fetch_europeana_data_with_themes(args):
    """Fetch and aggregate data by DATA_PROVIDER, LEGAL_TOOL, and THEME."""
    LOGGER.info("Fetching aggregated Europeana data with themes.")

    if not EUROPEANA_API_KEY:
        raise shared.QuantifyingException(
            "EUROPEANA_API_KEY not found in environment variables", 1
        )

    # Themes from Europeana site
    themes = [
        "art",
        "fashion",
        "music",
        "industrial",
        "sport",
        "photography",
        "archaeology",
    ]

    items_per_query = max(20, args.limit // len(themes))
    all_items = []
    session = get_requests_session()

    for theme in themes:
        params = {
            "wskey": EUROPEANA_API_KEY,
            "rows": min(items_per_query, 20),
            "profile": "rich",
            "query": "*",
            "theme": theme,
        }

        try:
            LOGGER.info(
                f"Fetching {params['rows']} records for theme: '{theme}'"
            )
            with session.get(BASE_URL, params=params, timeout=30) as response:
                response.raise_for_status()
                results = response.json()
                items = results.get("items", [])
            for item in items:
                item["theme_used"] = theme
            all_items.extend(items)
            LOGGER.info(f"Retrieved {len(items)} items for theme '{theme}'")
            time.sleep(1)
        except requests.RequestException as e:
            LOGGER.warning(f"Failed to fetch data for theme '{theme}': {e}")
            continue

    if not all_items:
        LOGGER.error("No items retrieved for any theme.")
        return []

    LOGGER.info(f"Total items retrieved across all themes: {len(all_items)}")

    # Aggregate by DATA_PROVIDER + LEGAL_TOOL + THEME
    aggregation = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for item in all_items:
        data_providers = item.get("dataProvider", [])
        data_provider = (
            data_providers
            if isinstance(data_providers, str)
            else (
                data_providers[0]
                if isinstance(data_providers, list) and data_providers
                else "Unknown"
            )
        )

        rights = item.get("rights", [])
        legal_tool = (
            rights
            if isinstance(rights, str)
            else (
                rights[0] if isinstance(rights, list) and rights else "Unknown"
            )
        )

        if (
            legal_tool
            and isinstance(legal_tool, str)
            and legal_tool.startswith("http")
        ):
            parts = legal_tool.strip("/").split("/")
            last_parts = parts[-2:]
            if last_parts:
                joined = " ".join(part.upper() for part in last_parts if part)
                if "creativecommons.org" in legal_tool:
                    legal_tool = f"CC {joined}"
                else:
                    legal_tool = joined
            else:
                legal_tool = "Unknown"

        theme = item.get("theme_used", "Unknown")
        aggregation[data_provider][legal_tool][theme] += 1

    # Convert to flat list
    output = []
    for provider, licenses in aggregation.items():
        for legal_tool, themes_dict in licenses.items():
            for theme, count in themes_dict.items():
                output.append(
                    {
                        "DATA_PROVIDER": provider,
                        "LEGAL_TOOL": legal_tool,
                        "THEME": theme,
                        "COUNT": count,
                    }
                )

    LOGGER.info(f"Aggregated data with themes into {len(output)} records.")
    return output


def save_csv(filepath, header, data):
    """Save data to a CSV file."""
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)
    LOGGER.info(f"Saved {len(data)} rows to {filepath}.")


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    os.makedirs(PATHS["data_phase"], exist_ok=True)

    # Fetch and save data WITHOUT themes (aggregated)
    data_no_theme = fetch_europeana_data_without_themes(args)
    if args.enable_save and data_no_theme:
        save_csv(FILE_WITHOUT_THEMES, HEADER_WITHOUT_THEMES, data_no_theme)

    # Fetch and save data WITH themes (aggregated)
    data_with_theme = fetch_europeana_data_with_themes(args)
    if args.enable_save and data_with_theme:
        save_csv(FILE_WITH_THEMES, HEADER_WITH_THEMES, data_with_theme)

    # Git commit & push
    if args.enable_git and args.enable_save:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add Europeana files (with and without themes) for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])

    LOGGER.info(
        "Europeana dual-fetch (with & without themes) completed successfully."
    )


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

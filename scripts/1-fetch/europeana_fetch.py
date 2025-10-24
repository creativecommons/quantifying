#!/usr/bin/env python
"""
Fetch high-level Europeana statistics for Quantifying the Commons.
Generates two datasets:
1) Without themes (aggregated by DATA_PROVIDER, LEGAL_TOOL)
2) With all themes (aggregated by DATA_PROVIDER, LEGAL_TOOL, THEME)
Uses totalResults instead of looping through pages for efficiency.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback

# Third-party
import requests
from dotenv import load_dotenv
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter, Retry

# Add parent directory for shared imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)
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
LOGGER.info(
    "Optimized Europeana dual-fetch (using totalResults) script started."
)


def parse_arguments():
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
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
    """Create a requests session with retry."""
    max_retries = Retry(
        total=5, backoff_factor=5, status_forcelist=shared.STATUS_FORCELIST
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update(
        {"accept": "application/json", "User-Agent": shared.USER_AGENT}
    )
    return session


def get_facet_list(session, facet_field):
    """Fetch complete facet list"""
    all_values = []
    offset = 0
    limit = 100

    LOGGER.info(f"Fetching {facet_field} facet values.")

    while True:
        params = {
            "wskey": EUROPEANA_API_KEY,
            "query": "*",
            "rows": 0,
            "facet": facet_field,
            f"f.{facet_field}.facet.limit": limit,
            f"f.{facet_field}.facet.offset": offset,
            "profile": "facets",
        }

        resp = session.get(BASE_URL, params=params, timeout=30)
        data = resp.json()

        facets = data.get("facets", [])
        if not facets or not facets[0].get("fields"):
            break

        fields = facets[0]["fields"]
        new_values = [f["label"] for f in fields if f.get("label")]

        for v in new_values:
            if v not in all_values:
                all_values.append(v)

        if len(new_values) < limit:
            break

        offset += limit
        time.sleep(0.5)

    LOGGER.info(
        f"Completed fetching {facet_field}. Total unique: {len(all_values)}"
    )

    return all_values


def simplify_legal_tool(legal_tool):
    """
    Simplify and standardize license URLs (especially Creative Commons).

    This function converts long or complex license URLs into
    short, human-readable labels like "CC BY-SA 4.0" or "CC BY-NC-ND 3.0 AT".

    It handles both:
    - Non-ported Creative Commons licenses (no jurisdiction)
    - Ported licenses (with jurisdiction codes, e.g., 'AT', 'DE', etc.)
    - Other license URLs gracefully (returns simplified or original form)

    Examples
    --------
    >>> simplify_legal_tool("http://creativecommons.org/licenses/by-sa/4.0/")
    'CC BY-SA 4.0'

    >>> simplify_legal_tool
    >>> ("http://creativecommons.org/licenses/by-nc-nd/3.0/at/")
    'CC BY-NC-ND 3.0 AT'

    >>> simplify_legal_tool("http://rightsstatements.org/vocab/InC/1.0/")
    'VOCAB INC 1.0'

    >>> simplify_legal_tool("Public Domain")
    'Public Domain'

    Parameters
    ----------
    legal_tool : str
        A license string or URL, e.g., Creative Commons or RightsStatement.

    Returns
    -------
    str
        A short, standardized form of the license.
    """
    if not (legal_tool and isinstance(legal_tool, str)):
        return legal_tool

    if legal_tool.startswith("http"):
        parts = legal_tool.strip("/").split("/")
        last_parts = parts[-3:]  # allow for jurisdiction at the end

        # Detect jurisdiction (2-letter code or known suffix)
        jurisdiction = ""
        if len(last_parts[-1]) == 2 and last_parts[-1].isalpha():
            jurisdiction = last_parts[-1].upper()
            last_parts = last_parts[:-1]

        # Join and format
        joined = " ".join(
            part.upper() for part in last_parts if part and part != "licenses"
        )

        if "creativecommons.org" in legal_tool:
            return f"CC {joined} {jurisdiction}".strip()
        else:
            return joined or "Unknown"

    return legal_tool


def fetch_europeana_data_without_themes(session, providers, rights_list):
    """Fetch aggregated counts by DATA_PROVIDER and LEGAL_TOOL (no theme)

    Parameters
    ----------
    session : requests.Session
        A configured requests session.
    providers : list[str]
        List of DATA_PROVIDER names.
    rights_list : list[str]
        List of license/rights strings.

    """
    LOGGER.info(
        "Fetching Europeana totalResults aggregated "
        "by provider and rights (without themes)."
    )

    output = []
    for provider in providers:
        for rights in rights_list:
            params = {
                "wskey": EUROPEANA_API_KEY,
                "rows": 0,
                "query": f'DATA_PROVIDER:"{provider}" AND RIGHTS:"{rights}"',
            }
            try:
                resp = session.get(BASE_URL, params=params, timeout=30)
                resp.raise_for_status()
                count = resp.json().get("totalResults", 0)

                if count > 0:
                    simplified_rights = simplify_legal_tool(rights)
                    output.append(
                        {
                            "DATA_PROVIDER": provider,
                            "LEGAL_TOOL": simplified_rights,
                            "COUNT": count,
                        }
                    )
            except requests.RequestException as e:
                LOGGER.warning(
                    f"Failed for provider={provider}, rights={rights}: {e}"
                )
            time.sleep(0.5)

    LOGGER.info(f"Aggregated {len(output)} records (without themes).")
    return output


def fetch_europeana_data_with_themes(session, providers, rights_list, themes):
    """
    Fetch aggregated counts by DATA_PROVIDER, LEGAL_TOOL, and THEME.

    Parameters
    ----------
    session : requests.Session
        A configured requests session.
    providers : list[str]
        List of DATA_PROVIDER names.
    rights_list : list[str]
        List of license/rights strings.
    themes : list[str]
        List of themes to query.

    Returns
    -------
    list[dict]
        Aggregated counts with keys: DATA_PROVIDER, LEGAL_TOOL, THEME, COUNT.
    """
    LOGGER.info(
        "Fetching Europeana totalResults "
        "aggregated by provider, rights, and theme."
    )

    output = []

    for provider in providers:
        for rights in rights_list:
            simplified_rights = simplify_legal_tool(rights)
            for theme in themes:  # use the themes passed from main()
                params = {
                    "wskey": EUROPEANA_API_KEY,
                    "rows": 0,
                    "query": (
                        f'DATA_PROVIDER:"{provider}" ' f'AND RIGHTS:"{rights}"'
                    ),
                    "theme": theme,
                }
                try:
                    resp = session.get(BASE_URL, params=params, timeout=30)
                    resp.raise_for_status()
                    count = resp.json().get("totalResults", 0)
                    if count > 0:
                        output.append(
                            {
                                "DATA_PROVIDER": provider,
                                "LEGAL_TOOL": simplified_rights,
                                "THEME": theme,
                                "COUNT": count,
                            }
                        )
                except requests.RequestException as e:
                    LOGGER.warning(
                        f"Failed for provider={provider}, rights={rights}, "
                        f"theme={theme}: {e}"
                    )
                time.sleep(0.5)

    LOGGER.info(f"Aggregated {len(output)} records (with themes).")
    return output


def save_csv(filepath, header, data):
    """Save aggregated data to CSV."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)
    LOGGER.info(f"Saved {len(data)} rows to {filepath}.")


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    # --- Environment check for Europeana API key ---
    if not EUROPEANA_API_KEY:
        raise shared.QuantifyingException(
            "EUROPEANA_API_KEY not found in environment variables", 1
        )

    session = get_requests_session()

    providers = get_facet_list(session, "DATA_PROVIDER")
    rights_list = get_facet_list(session, "RIGHTS")

    # Define themes here (alphabetically for consistency)
    themes = [
        "archaeology",
        "art",
        "fashion",
        "industrial",
        "manuscript",
        "maps",
        "migration",
        "music",
        "nature",
        "newspaper",
        "photography",
        "sport",
        "ww1",
    ]

    # Fetch data
    data_no_theme = fetch_europeana_data_without_themes(
        session, providers, rights_list
    )
    data_with_theme = fetch_europeana_data_with_themes(
        session, providers, rights_list, themes
    )

    # Save if enabled
    if args.enable_save:
        if data_no_theme:
            save_csv(FILE_WITHOUT_THEMES, HEADER_WITHOUT_THEMES, data_no_theme)
        if data_with_theme:
            save_csv(FILE_WITH_THEMES, HEADER_WITH_THEMES, data_with_theme)

    # Git actions
    if args.enable_git and args.enable_save:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add Europeana files (with and without themes) for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])

    LOGGER.info("Optimized Europeana dual-fetch completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
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
